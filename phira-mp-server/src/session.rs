use crate::{
    Chart, InternalRoomState, Record, Room, ServerState,
    l10n::{LANGUAGE, Language},
    tl,
};
use anyhow::{anyhow, bail, Context, Result};
use phira_mp_common::{
    ClientCommand, JoinRoomResponse, Message, RoomState, ServerCommand, Stream, UserInfo,
    HEARTBEAT_DISCONNECT_TIMEOUT,
};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    ops::DerefMut,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    net::TcpStream,
    sync::{Mutex, Notify, OnceCell, RwLock, oneshot},
    task::JoinHandle,
    time,
};
use tracing::{Instrument, debug, debug_span, error, info, trace, warn};
use uuid::Uuid;

const HOST: &str = "https://phira.5wyxi.com";

pub struct User {
    pub id: i32,
    pub name: String,
    pub lang: Language,

    pub server: Arc<ServerState>,
    pub session: RwLock<Option<Weak<Session>>>,
    pub room: RwLock<Option<Arc<Room>>>,

    pub monitor: AtomicBool,
    pub console: AtomicBool,
    pub game_time: AtomicU32,

    pub dangle_mark: Mutex<Option<Arc<()>>>,
}

impl User {
    pub fn new(id: i32, name: String, lang: Language, server: Arc<ServerState>) -> Self {
        Self {
            id,
            name,
            lang,

            server,
            session: RwLock::default(),
            room: RwLock::default(),

            monitor: AtomicBool::default(),
            console: AtomicBool::default(),
            game_time: AtomicU32::default(),

            dangle_mark: Mutex::default(),
        }
    }

    pub fn to_info(&self) -> UserInfo {
        UserInfo {
            id: self.id,
            name: self.name.clone(),
            monitor: self.monitor.load(Ordering::SeqCst),
        }
    }

    pub fn can_monitor(&self) -> bool {
        self.server.config.monitors.contains(&self.id)
    }

    pub async fn set_session(&self, session: Weak<Session>) {
        *self.session.write().await = Some(session);
        *self.dangle_mark.lock().await = None;
    }

    pub async fn try_send(&self, cmd: ServerCommand) {
        if let Some(session) = self.session.read().await.as_ref().and_then(Weak::upgrade) {
            session.try_send(cmd).await;
        } else {
            warn!("sending {cmd:?} to dangling user {}", self.id);
        }
    }

    pub async fn dangle(self: Arc<Self>) {
        warn!(user = self.id, "user dangling");
        let guard = self.room.read().await;
        let room = guard.as_ref().map(Arc::clone);
        drop(guard);
        if let Some(room) = room {
            let guard = room.state.read().await;
            if matches!(*guard, InternalRoomState::Playing { .. }) {
                warn!(user = self.id, "lost connection on playing, aborting");
                self.server.users.write().await.remove(&self.id);
                drop(guard);
                if room.on_user_leave(&self).await {
                    self.server.rooms.write().await.remove(&room.id);
                }
                return;
            }
        }
        let dangle_mark = Arc::new(());
        *self.dangle_mark.lock().await = Some(Arc::clone(&dangle_mark));
        tokio::spawn(async move {
            time::sleep(Duration::from_secs(10)).await;
            if Arc::strong_count(&dangle_mark) > 1 {
                let guard = self.room.read().await;
                let room = guard.as_ref().map(Arc::clone);
                drop(guard);
                if let Some(room) = room {
                    self.server.users.write().await.remove(&self.id);
                    if room.on_user_leave(&self).await {
                        self.server.rooms.write().await.remove(&room.id);
                    }
                }
            }
        });
    }
}

pub enum SessionCategory {
    Normal,
    Console,
    RoomMonitor,
    GameMonitor,
}

pub struct Session {
    pub id: Uuid,
    pub category: SessionCategory,
    pub stream: Stream<ServerCommand, ClientCommand>,
    pub user: Arc<User>,

    monitor_task_handle: JoinHandle<()>,
}

#[derive(Debug, Deserialize)]
struct AuthUserInfo {
    pub id: i32,
    pub name: String,
    pub language: String,
}

impl Session {
    pub async fn new(id: Uuid, stream: TcpStream, server: Arc<ServerState>) -> Result<Arc<Self>> {
        stream.set_nodelay(true)?;
        let this = Arc::new(OnceCell::<Arc<Session>>::new());
        let this_inited = Arc::new(Notify::new());
        let (tx, rx) = oneshot::channel::<(Arc<User>, SessionCategory)>();
        let last_recv: Arc<Mutex<Instant>> = Arc::new(Mutex::new(Instant::now()));
        let stream = Stream::<ServerCommand, ClientCommand>::new(
            None,
            stream,
            Box::new({
                let this = Arc::clone(&this);
                let this_inited = Arc::clone(&this_inited);
                let mut tx = Some(tx);
                let server = Arc::clone(&server);
                let last_recv = Arc::clone(&last_recv);
                let waiting_for_authenticate = Arc::new(AtomicBool::new(true));
                let panicked = Arc::new(AtomicBool::new(false));
                move |send_tx, cmd| {
                    session_handler(
                        id,
                        Arc::clone(&this),
                        Arc::clone(&this_inited),
                        tx.take(),
                        Arc::clone(&server),
                        Arc::clone(&last_recv),
                        Arc::clone(&waiting_for_authenticate),
                        Arc::clone(&panicked),
                        send_tx,
                        cmd,
                    )
                }
            }),
        )
        .await?;
        let monitor_task_handle = tokio::spawn({
            let server = Arc::clone(&server);
            let last_recv = Arc::clone(&last_recv);
            async move {
                loop {
                    let recv = *last_recv.lock().await;
                    time::sleep_until((recv + HEARTBEAT_DISCONNECT_TIMEOUT).into()).await;

                    if *last_recv.lock().await + HEARTBEAT_DISCONNECT_TIMEOUT > Instant::now() {
                        continue;
                    }

                    if let Err(err) = server.lost_con_tx.send(id).await {
                        error!("failed to mark lost connection ({id}): {err:?}");
                    }
                    break;
                }
            }
        });

        let (user, category) = rx.await?;
        let res = Arc::new(Self {
            id,
            category,
            stream,
            user,
            monitor_task_handle,
        });
        let _ = this.set(Arc::clone(&res));
        this_inited.notify_one();
        Ok(res)
    }

    pub fn version(&self) -> u8 {
        self.stream.version()
    }

    pub fn name(&self) -> &str {
        &self.user.name
    }

    pub async fn try_send(&self, cmd: ServerCommand) {
        if let Err(err) = self.stream.send(cmd).await {
            error!("failed to deliver command to {}: {err:?}", self.id);
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.monitor_task_handle.abort();
    }
}

async fn authenticate(id: Uuid, token: &str) -> Result<AuthUserInfo> {
    debug!("session {id}: authenticate {token}");
    reqwest::Client::new()
        .get(format!("{HOST}/me"))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .with_context(|| "failed to fetch info")?
        .json()
        .await
        .inspect_err(|e| warn!("failed to fetch info: {e:?}"))
        .with_context(|| "failed to fetch info")
}

async fn session_handler(
    id: Uuid,
    this: Arc<OnceCell<Arc<Session>>>,
    this_inited: Arc<Notify>,
    tx: Option<oneshot::Sender<(Arc<User>, SessionCategory)>>,
    server: Arc<ServerState>,
    last_recv: Arc<Mutex<Instant>>,
    waiting_for_authenticate: Arc<AtomicBool>,
    panicked: Arc<AtomicBool>,
    send_tx: Arc<tokio::sync::mpsc::Sender<ServerCommand>>,
    cmd: ClientCommand,
) {
    if panicked.load(Ordering::SeqCst) {
        return;
    }
    *last_recv.lock().await = Instant::now();
    if matches!(cmd, ClientCommand::Ping) {
        let _ = send_tx.send(ServerCommand::Pong).await;
        return;
    }
    if !waiting_for_authenticate.load(Ordering::SeqCst) {
        let user = this.get().map(|it| Arc::clone(&it.user)).unwrap();
        if let Some(resp) = LANGUAGE
            .scope(Arc::new(user.lang.clone()), process(user, cmd))
            .await
        {
            if let Err(err) = send_tx.send(resp).await {
                error!("failed to handle message, aborting connection {id}: {err:?}",);
                panicked.store(true, Ordering::SeqCst);
                if let Err(err) = server.lost_con_tx.send(id).await {
                    error!("failed to mark lost connection ({id}): {err:?}");
                }
            }
        }
        return;
    }
    match cmd {
        ClientCommand::Authenticate { ref token }
        | ClientCommand::GameMonitorAuthenticate { ref token } => {
            // normal game client
            let Some(tx) = tx else { return };
            match authenticate(id, token).await {
                Ok(resp) => {
                    let mut users_guard = server.users.write().await;
                    let (cat, resp) = match cmd {
                        ClientCommand::Authenticate { .. } => (SessionCategory::Normal, resp),
                        ClientCommand::GameMonitorAuthenticate { .. } => (
                            SessionCategory::GameMonitor,
                            AuthUserInfo {
                                id: -resp.id,
                                name: resp.name + " (monitor)",
                                language: resp.language,
                            },
                        ),
                        _ => unreachable!(),
                    };
                    if let Some(user) = users_guard.get(&resp.id) {
                        info!("reconnect");
                        let _ = tx.send((Arc::clone(user), cat));
                        this_inited.notified().await;
                        user.set_session(Arc::downgrade(this.get().unwrap())).await;
                    } else {
                        let user = Arc::new(User::new(
                            resp.id,
                            resp.name,
                            resp.language.parse().map(Language).unwrap_or_default(),
                            Arc::clone(&server),
                        ));
                        let _ = tx.send((Arc::clone(&user), cat));
                        this_inited.notified().await;
                        user.set_session(Arc::downgrade(this.get().unwrap())).await;
                        users_guard.insert(resp.id, user);
                    }

                    let user = &this.get().unwrap().user;
                    let room_state = match user.room.read().await.as_ref() {
                        Some(room) => Some(room.client_state(user).await),
                        None => None,
                    };
                    let _ = send_tx
                        .send(ServerCommand::Authenticate(Ok((
                            user.to_info(),
                            room_state,
                        ))))
                        .await;
                    waiting_for_authenticate.store(false, Ordering::SeqCst);
                }
                Err(err) => {
                    warn!("failed to authenticate: {err:?}");
                    let _ = send_tx
                        .send(ServerCommand::Authenticate(Err(err.to_string())))
                        .await;
                    panicked.store(true, Ordering::SeqCst);
                    if let Err(err) = server.lost_con_tx.send(id).await {
                        error!("failed to mark lost connection ({id}): {err:?}");
                    }
                }
            }
        }
        ClientCommand::ConsoleAuthenticate { ref token } => {
            // a server console client
            let Some(tx) = tx else { return };
            match authenticate(id, token).await {
                Ok(resp) => {
                    let user = Arc::new(User::new(
                        resp.id,
                        resp.name,
                        resp.language.parse().map(Language).unwrap_or_default(),
                        Arc::clone(&server),
                    ));
                    let _ = tx.send((Arc::clone(&user), SessionCategory::Console));
                    this_inited.notified().await;
                    user.set_session(Arc::downgrade(this.get().unwrap())).await;

                    let _ = send_tx
                        .send(ServerCommand::Authenticate(Ok((user.to_info(), None))))
                        .await;
                    waiting_for_authenticate.store(false, Ordering::SeqCst);
                }
                Err(err) => {
                    warn!("failed to authenticate: {err:?}");
                    let _ = send_tx
                        .send(ServerCommand::Authenticate(Err(err.to_string())))
                        .await;
                    panicked.store(true, Ordering::SeqCst);
                    if let Err(err) = server.lost_con_tx.send(id).await {
                        error!("failed to mark lost connection ({id}): {err:?}");
                    }
                }
            }
        }
        ClientCommand::RoomMonitorAuthenticate { key } => {
            // a room monitor client
            let Some(tx) = tx else { return };
            let err = if server.get_room_monitor().await.is_some() {
                Some("more than one room monitor")
            } else if server.room_monitor_key == key {
                None
            } else {
                Some("secret key mismatch")
            };
            if let Some(str) = err {
                warn!("authentication failed: {str}");
                let _ = send_tx
                    .send(ServerCommand::Authenticate(Err(str.into())))
                    .await;
                panicked.store(true, Ordering::SeqCst);
                if let Err(err) = server.lost_con_tx.send(id).await {
                    error!("failed to mark lost connection ({id}): {err:?}");
                }
            } else {
                info!("new room monitor connected");
                let user = Arc::new(User::new(
                    -1,
                    "$server_room_monitor".into(),
                    Default::default(),
                    Arc::clone(&server),
                ));
                let _ = send_tx
                    .send(ServerCommand::Authenticate(Ok((user.to_info(), None))))
                    .await;
                let _ = tx.send((Arc::clone(&user), SessionCategory::RoomMonitor));
                this_inited.notified().await;

                let weak_this = Arc::downgrade(this.get().unwrap());
                user.set_session(Weak::clone(&weak_this)).await;
                server.room_monitor.write().await.replace(weak_this);

                waiting_for_authenticate.store(false, Ordering::SeqCst);
            }
        }
        _ => warn!("packet before authentication, ignoring: {cmd:?}"),
    }
}

async fn process(user: Arc<User>, cmd: ClientCommand) -> Option<ServerCommand> {
    #[inline]
    fn err_to_str<T>(result: Result<T>) -> Result<T, String> {
        result.map_err(|it| it.to_string())
    }

    macro_rules! get_room {
        (~ $d:ident) => {
            let $d = match user.room.read().await.as_ref().map(Arc::clone) {
                Some(room) => room,
                None => {
                    warn!("no room");
                    return None;
                }
            };
        };
        ($d:ident) => {
            let $d = user
                .room
                .read()
                .await
                .as_ref()
                .map(Arc::clone)
                .ok_or_else(|| anyhow!("no room"))?;
        };
        ($d:ident, $($pt:tt)*) => {
            let $d = user
                .room
                .read()
                .await
                .as_ref()
                .map(Arc::clone)
                .ok_or_else(|| anyhow!("no room"))?;
            if !matches!(&*$d.state.read().await, $($pt)*) {
                bail!("invalid state");
            }
        };
    }
    macro_rules! send_room_event {
        ($event_type:literal, $data:tt $(,)?) => {
            if let Some(p) = user.server.get_room_monitor().await {
                p.try_send(ServerCommand::RoomEvent {
                    event_type: $event_type.to_string(),
                    data: json!($data),
                })
                .await;
            }
        };
    }
    macro_rules! category_matches {
        ($pattern:pat) => {
            user.session
                .read()
                .await
                .as_ref()
                .and_then(|p| p.upgrade())
                .is_some_and(|s| matches!(s.category, $pattern))
        };
    }
    match cmd {
        ClientCommand::Ping => unreachable!(),
        ClientCommand::Authenticate { .. }
        | ClientCommand::ConsoleAuthenticate { .. }
        | ClientCommand::RoomMonitorAuthenticate { .. }
        | ClientCommand::GameMonitorAuthenticate { .. } => Some(ServerCommand::Authenticate(Err(
            "repeated authenticate".to_owned(),
        ))),
        ClientCommand::Chat { message } => {
            let res: Result<()> = async move {
                get_room!(room);
                room.send_as(&user, message.into_inner()).await;
                Ok(())
            }
            .await;
            Some(ServerCommand::Chat(err_to_str(res)))
        }
        ClientCommand::Touches { frames } => {
            get_room!(~ room);
            if room.is_live() {
                debug!("received {} touch events from {}", frames.len(), user.id);
                if let Some(frame) = frames.last() {
                    user.game_time.store(frame.time.to_bits(), Ordering::SeqCst);
                }
                tokio::spawn(async move {
                    room.broadcast_monitors(ServerCommand::Touches {
                        player: user.id,
                        frames,
                    })
                    .await;
                });
            } else {
                warn!("received touch events in non-live mode");
            }
            None
        }
        ClientCommand::Judges { judges } => {
            get_room!(~ room);
            if room.is_live() {
                debug!("received {} judge events from {}", judges.len(), user.id);
                tokio::spawn(async move {
                    room.broadcast_monitors(ServerCommand::Judges {
                        player: user.id,
                        judges,
                    })
                    .await;
                });
            } else {
                warn!("received judge events in non-live mode");
            }
            None
        }
        ClientCommand::CreateRoom { id } => {
            let res: Result<()> = async move {
                if !category_matches!(SessionCategory::Normal) {
                    bail!("invalid command for this session");
                }
                let mut room_guard = user.room.write().await;
                if room_guard.is_some() {
                    bail!("already in room");
                }

                let mut map_guard = user.server.rooms.write().await;
                let room = Arc::new(Room::new(id.clone(), Arc::downgrade(&user)));
                match map_guard.entry(id.clone()) {
                    Entry::Vacant(entry) => {
                        entry.insert(Arc::clone(&room));
                    }
                    Entry::Occupied(_) => {
                        bail!(tl!("create-id-occupied"));
                    }
                }
                drop(map_guard);

                room.send(Message::CreateRoom { user: user.id }).await;
                user.try_send(ServerCommand::Message(Message::Chat {
                    user: 1,
                    content: "欢迎进入HSNPhira服务器！凌晨1：30服务器重启哦".to_string(),
                }))
                .await;
                user.try_send(ServerCommand::Message(Message::Chat {
                    user: 1,
                    content: "想要查询房间？加入1049578201交流群即可查询！".to_string(),
                }))
                .await;

                send_room_event!("create_room", {
                    "room": id.to_string(),
                    "data": room.into_json().await,
                });
                *room_guard = Some(room);
                info!(user = user.id, room = id.to_string(), "user create room");
                Ok(())
            }
            .await;
            Some(ServerCommand::CreateRoom(err_to_str(res)))
        }
        ClientCommand::JoinRoom { id, monitor } => {
            let res: Result<JoinRoomResponse> = async move {
                if !category_matches!(SessionCategory::Normal | SessionCategory::GameMonitor) {
                    bail!("invalid command for this session");
                }
                if !monitor && category_matches!(SessionCategory::GameMonitor) {
                    bail!("monitor = false, but you are in game monitor session");
                }
                let mut room_guard = user.room.write().await;
                if room_guard.is_some() {
                    bail!("already in room");
                }
                let room = user.server.rooms.read().await.get(&id).map(Arc::clone);
                let Some(room) = room else {
                    bail!("room not found")
                };
                if room.locked.load(Ordering::SeqCst) {
                    bail!(tl!("join-room-locked"));
                }
                if !matches!(*room.state.read().await, InternalRoomState::SelectChart) {
                    bail!(tl!("join-game-ongoing"));
                }
                if monitor && !category_matches!(SessionCategory::GameMonitor) {
                    bail!(tl!("join-cant-monitor"));
                }
                if !room.add_user(Arc::downgrade(&user), monitor).await {
                    bail!(tl!("join-room-full"));
                }
                info!(
                    user = user.id,
                    room = id.to_string(),
                    monitor,
                    "user join room"
                );
                user.monitor.store(monitor, Ordering::SeqCst);
                if monitor && !room.live.fetch_or(true, Ordering::SeqCst) {
                    info!(room = id.to_string(), "room goes live");
                }
                room.broadcast(ServerCommand::OnJoinRoom(user.to_info()))
                    .await;
                room.send(Message::JoinRoom {
                    user: user.id,
                    name: user.name.clone(),
                })
                .await;

                if !monitor {
                    send_room_event!("join_room", {
                        "room": id.to_string(),
                        "user": user.id,
                    });
                }
                *room_guard = Some(Arc::clone(&room));
                Ok(JoinRoomResponse {
                    state: room.client_room_state().await,
                    users: room
                        .users()
                        .await
                        .into_iter()
                        .chain(room.monitors().await.into_iter())
                        .map(|it| it.to_info())
                        .collect(),
                    live: room.is_live(),
                })
            }
            .await;
            Some(ServerCommand::JoinRoom(err_to_str(res)))
        }
        ClientCommand::LeaveRoom => {
            let res: Result<()> = async move {
                if !category_matches!(SessionCategory::Normal | SessionCategory::GameMonitor) {
                    bail!("invalid command for this session");
                }
                get_room!(room);
                // TODO is this necessary?
                // if !matches!(*room.state.read().await, InternalRoomState::SelectChart) {
                // bail!("game ongoing, can't leave");
                // }
                info!(
                    user = user.id,
                    room = room.id.to_string(),
                    "user leave room"
                );
                if room.on_user_leave(&user).await {
                    user.server.rooms.write().await.remove(&room.id);
                }

                send_room_event!("leave_room", {
                    "room": room.id.to_string(),
                    "user": user.id,
                });
                Ok(())
            }
            .await;
            Some(ServerCommand::LeaveRoom(err_to_str(res)))
        }
        ClientCommand::LockRoom { lock } => {
            let res: Result<()> = async move {
                if !category_matches!(SessionCategory::Normal) {
                    bail!("invalid command for this session");
                }
                get_room!(room);
                room.check_host(&user).await?;
                info!(
                    user = user.id,
                    room = room.id.to_string(),
                    lock,
                    "lock room"
                );
                room.locked.store(lock, Ordering::SeqCst);
                room.send(Message::LockRoom { lock }).await;

                send_room_event!("update_room", {
                    "room": room.id.to_string(),
                    "data": json!({"lock": lock}),
                });
                Ok(())
            }
            .await;
            Some(ServerCommand::LockRoom(err_to_str(res)))
        }
        ClientCommand::CycleRoom { cycle } => {
            let res: Result<()> = async move {
                if !category_matches!(SessionCategory::Normal) {
                    bail!("invalid command for this session");
                }
                get_room!(room);
                room.check_host(&user).await?;
                info!(
                    user = user.id,
                    room = room.id.to_string(),
                    cycle,
                    "cycle room"
                );
                room.cycle.store(cycle, Ordering::SeqCst);
                room.send(Message::CycleRoom { cycle }).await;

                send_room_event!("update_room", {
                    "room": room.id.to_string(),
                    "data": json!({"cycle": cycle})
                });
                Ok(())
            }
            .await;
            Some(ServerCommand::CycleRoom(err_to_str(res)))
        }
        ClientCommand::SelectChart { id } => {
            let res: Result<()> = async move {
                if !category_matches!(SessionCategory::Normal) {
                    bail!("invalid command for this session");
                }
                get_room!(room, InternalRoomState::SelectChart);
                room.check_host(&user).await?;
                let span = debug_span!(
                    "select chart",
                    user = user.id,
                    room = room.id.to_string(),
                    chart = id,
                );
                async move {
                    trace!("fetch");
                    let res: Chart = reqwest::get(format!("{HOST}/chart/{id}"))
                        .await?
                        .error_for_status()?
                        .json()
                        .await?;
                    debug!("chart is {res:?}");
                    room.send(Message::SelectChart {
                        user: user.id,
                        name: res.name.clone(),
                        id: res.id,
                    })
                    .await;
                    *room.chart.write().await = Some(res);
                    room.on_state_change().await;

                    send_room_event!("update_room", {
                        "room": room.id.to_string(),
                        "data": json!({"chart": id})
                    });
                    Ok(())
                }
                .instrument(span)
                .await
            }
            .await;
            Some(ServerCommand::SelectChart(err_to_str(res)))
        }

        ClientCommand::RequestStart => {
            let res: Result<()> = async move {
                if !category_matches!(SessionCategory::Normal) {
                    bail!("invalid command for this session");
                }
                get_room!(room, InternalRoomState::SelectChart);
                room.check_host(&user).await?;
                if room.chart.read().await.is_none() {
                    bail!(tl!("start-no-chart-selected"));
                }
                debug!(room = room.id.to_string(), "room wait for ready");
                room.reset_game_time().await;
                room.send(Message::GameStart { user: user.id }).await;
                *room.state.write().await = InternalRoomState::WaitForReady {
                    started: std::iter::once(user.id).collect::<HashSet<_>>(),
                };
                room.on_state_change().await;
                room.check_all_ready().await;

                let state_str = match *room.state.read().await {
                    InternalRoomState::Playing { .. } => "PLAYING",
                    _ => "WAITING_FOR_READY",
                };
                send_room_event!("update_room", {
                    "room": room.id.to_string(),
                    "data": json!({"state": state_str})
                });
                Ok(())
            }
            .await;
            Some(ServerCommand::RequestStart(err_to_str(res)))
        }
        ClientCommand::Ready => {
            let res: Result<()> = async move {
                if !category_matches!(SessionCategory::Normal | SessionCategory::GameMonitor) {
                    bail!("invalid command for this session");
                }
                get_room!(room);
                let mut guard = room.state.write().await;
                if let InternalRoomState::WaitForReady { started } = guard.deref_mut() {
                    if !started.insert(user.id) {
                        bail!("already ready");
                    }
                    room.send(Message::Ready { user: user.id }).await;
                    drop(guard);
                    room.check_all_ready().await;

                    if matches!(*room.state.read().await, InternalRoomState::Playing { .. }) {
                        send_room_event!("start_round", {
                            "room": room.id.to_string()
                        });
                        send_room_event!("update_room", {
                            "room": room.id.to_string(),
                            "data": json!({"state": "PLAYING"})
                        });
                    }
                }
                Ok(())
            }
            .await;
            Some(ServerCommand::Ready(err_to_str(res)))
        }
        ClientCommand::CancelReady => {
            let res: Result<()> = async move {
                if !category_matches!(SessionCategory::Normal) {
                    bail!("invalid command for this session");
                }
                get_room!(room);
                let mut guard = room.state.write().await;
                if let InternalRoomState::WaitForReady { started } = guard.deref_mut() {
                    if !started.remove(&user.id) {
                        bail!("not ready");
                    }
                    if room.check_host(&user).await.is_ok() {
                        room.send(Message::CancelGame { user: user.id }).await;
                        *guard = InternalRoomState::SelectChart;
                        drop(guard);
                        room.on_state_change().await;
                    } else {
                        room.send(Message::CancelReady { user: user.id }).await;
                    }
                }
                Ok(())
            }
            .await;
            Some(ServerCommand::CancelReady(err_to_str(res)))
        }
        ClientCommand::Played { id } => {
            let res: Result<()> = async move {
                if !category_matches!(SessionCategory::Normal) {
                    bail!("invalid command for this session");
                }
                get_room!(room);
                let res: Record = reqwest::get(format!("{HOST}/record/{id}"))
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                if res.player != user.id {
                    bail!("invalid record");
                }
                debug!(
                    room = room.id.to_string(),
                    user = user.id,
                    "user played: {res:?}"
                );
                room.send(Message::Played {
                    user: user.id,
                    score: res.score,
                    accuracy: res.accuracy,
                    full_combo: res.full_combo,
                })
                .await;
                let mut guard = room.state.write().await;
                if let InternalRoomState::Playing { results, aborted } = guard.deref_mut() {
                    if aborted.contains(&user.id) {
                        bail!("aborted");
                    }
                    if results.insert(user.id, res).is_some() {
                        bail!("already uploaded");
                    }
                    drop(guard);
                    room.check_all_ready().await;

                    if matches!(
                        *room.state.read().await,
                        InternalRoomState::SelectChart { .. }
                    ) {
                        if let Some(round) = room.rounds.read().await.last() {
                            send_room_event!("new_round", {
                                "room": room.id.to_string(),
                                "round": round,
                            });
                        }
                        send_room_event!("update_room", {
                            "room": room.id.to_string(),
                            "data": json!({"state": "SELECTING_CHART"})
                        });
                    }
                }
                Ok(())
            }
            .await;
            Some(ServerCommand::Played(err_to_str(res)))
        }
        ClientCommand::Abort => {
            let res: Result<()> = async move {
                if !category_matches!(SessionCategory::Normal) {
                    bail!("invalid command for this session");
                }
                get_room!(room);
                let mut guard = room.state.write().await;
                if let InternalRoomState::Playing { results, aborted } = guard.deref_mut() {
                    if results.contains_key(&user.id) {
                        bail!("already uploaded");
                    }
                    if !aborted.insert(user.id) {
                        bail!("aborted");
                    }
                    drop(guard);
                    room.send(Message::Abort { user: user.id }).await;
                    room.check_all_ready().await;

                    if matches!(
                        room.state.read().await.to_client(None),
                        RoomState::SelectChart(_)
                    ) {
                        send_room_event!("update_room", {
                            "room": room.id.to_string(),
                            "data": json!({"state": "SELECTING_CHART"})
                        });
                    }
                }
                Ok(())
            }
            .await;
            Some(ServerCommand::Abort(err_to_str(res)))
        }
        ClientCommand::QueryRoomInfo => {
            let res = async move {
                if !category_matches!(SessionCategory::RoomMonitor) {
                    bail!("invalid command for this session");
                }
                let mut info = HashMap::new();
                let mut user_room_map = HashMap::new();
                for (id, room) in user.server.rooms.read().await.iter() {
                    for uid in room.users().await {
                        user_room_map.insert(uid.id, id.clone());
                    }
                    info.insert(id.clone(), room.into_json().await);
                }
                Ok((info, user_room_map))
            }
            .await;
            Some(ServerCommand::RoomResponse(err_to_str(res)))
        }
    }
}
