use axum::{
    body::{Body, Bytes},
    extract::{FromRef, Path, State},
    handler::Handler,
    http::{
        header::{
            CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH, CONTENT_LOCATION, CONTENT_RANGE,
            CONTENT_TYPE, TRANSFER_ENCODING,
        },
        HeaderMap, HeaderName, Method, StatusCode,
    },
    response::IntoResponse,
    routing::{delete, get, put},
    Error, Router,
};
use core::pin::Pin;
use futures_core::{
    task::{Context, Poll, Waker},
    Stream,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

type AppState = State<MyState>;

// BYTES_PER_CHUNK must always be smaller than WRITE_BUFFER_CAPACITY!
const BYTES_PER_CHUNK: usize = 8;
const WRITE_BUFFER_CAPACITY: usize = 100; //in bytes
const CONTENT_HEADERS: [HeaderName; 6] = [
    CONTENT_ENCODING,
    CONTENT_LANGUAGE,
    CONTENT_LENGTH,
    CONTENT_LOCATION,
    CONTENT_RANGE,
    CONTENT_TYPE,
];

async fn delete_stream(State(state): AppState, Path(path): Path<String>) -> impl IntoResponse {
    let mut stream_map = state.connections.lock().await;
    let valid_id = stream_map.contains_key(&path);
    if valid_id {
        stream_map.remove(&path);
        delete_path_from_db(&state, &path).await;
        return Ok(StatusCode::NO_CONTENT);
    } else {
        return Err((StatusCode::NOT_FOUND, "Requested stream ID was not found!"));
    }
}

async fn id_is_completed(state: &MyState, path: &str) -> bool {
    let stream_map = state.connections.lock().await;
    match stream_map.get(path) {
        None => return false,
        Some(stream) => {
            let writer = stream.writer.write().await;
            return writer.ended;
        }
    }
}

async fn id_exists(state: &MyState, path: &str) -> bool {
    let stream_map = state.connections.lock().await;
    return stream_map.contains_key(path);
}

async fn manage_readers(stream: &StreamCon) {
    let readers = &mut stream.readers.lock().unwrap();
    for reader in readers.iter() {
        reader.clone().wake();
    }
    readers.clear();
}

async fn get_path_splits(path: &str) -> Vec<(String, String)> {
    let prepended_path = "/".to_owned() + &path;
    let exploded_path = prepended_path.split('/');
    let mut result = Vec::new();

    let mut key_buffer = "".to_string();
    let mut key_iterator = exploded_path.clone().enumerate().peekable();
    while let Some((i, key_word)) = key_iterator.next() {
        key_buffer += key_word;
        if key_iterator.peek().is_some() {
            key_buffer += "/";
        }

        let mut value_buffer = "".to_string();
        let mut value_iterator = exploded_path.clone().skip(i + 1).peekable();
        while let Some(value_word) = value_iterator.next() {
            value_buffer += value_word;
            if value_iterator.peek().is_some() {
                value_buffer += "/";
            }
        }
        result.push((key_buffer.clone(), value_buffer.clone()));
    }
    return result;
}

async fn delete_path_from_db(state: &MyState, path: &str) {
    let paths = get_path_splits(path).await;
    for (key, value) in paths.into_iter() {
        let mut path_database = state.path_database.lock().await;
        let key_entry = path_database.entry(key.clone()).or_default();
        if key_entry.len() < 2 {
            path_database.remove(&key);
        } else {
            key_entry.remove(&value);
        }
    }
}

async fn add_path_into_db(state: &MyState, path: &str) {
    let mut path_database = state.path_database.lock().await;
    let paths = get_path_splits(path).await;
    for (key, value) in paths.into_iter() {
        let key_entry = path_database.entry(key).or_default();
        key_entry.insert(value);
    }
}
async fn contains_content_headers(headers: &HeaderMap) -> bool {
    for key in CONTENT_HEADERS.iter() {
        if headers.contains_key(key) {
            return true;
        }
    }
    return false;
}
async fn stream_check(
    path: &String,
    payload: &Bytes,
    headers: &HeaderMap,
) -> Result<(), (StatusCode, String)> {
    let chunked_encoding =
        headers.contains_key(TRANSFER_ENCODING) && headers[TRANSFER_ENCODING] == "chunked";
    if payload.len() == 0 && !chunked_encoding {
        return Err((
            StatusCode::BAD_REQUEST,
            "You requested so save emptiness, you silly!".to_owned(),
        ));
    } else if path.ends_with('/') {
        return Err((
            StatusCode::BAD_REQUEST,
            "Destination path cannot end with '/'!".to_owned(),
        ));
    } else if path.contains("//") {
        return Err((
            StatusCode::BAD_REQUEST,
            "Destination path cannot contain '//'!".to_owned(),
        ));
    } else if contains_content_headers(headers).await {
        //RFC commanded PUT behaviour
        return Err((
            StatusCode::NOT_IMPLEMENTED,
            "Destination path cannot contain '//'!".to_owned(),
        ));
    } else {
        return Ok(());
    }
}
async fn accept_stream(
    State(state): AppState,
    Path(path): Path<String>,
    headers: HeaderMap,
    payload: Bytes,
) -> impl IntoResponse {
    if let Err(e) = stream_check(&path, &payload, &headers).await {
        return Err(e);
    }

    if id_is_completed(&state, &path).await {
        let mut stream_map = state.connections.lock().await;
        stream_map.remove(&path);
    }
    let created = !id_exists(&state, &path).await;
    let chunked_encoding =
        headers.contains_key(TRANSFER_ENCODING) && headers[TRANSFER_ENCODING] == "chunked";
    let mut buffer = Vec::with_capacity(WRITE_BUFFER_CAPACITY);
    let iters = payload.len() / BYTES_PER_CHUNK;

    for i in 0..(iters + 1) {
        let (payload_start, payload_end) = (
            i * BYTES_PER_CHUNK,
            std::cmp::min((i + 1) * BYTES_PER_CHUNK, payload.len()),
        );

        buffer.push(payload.slice(payload_start..payload_end));

        let end_of_request = (i + 1) > iters;
        if buffer.len() > WRITE_BUFFER_CAPACITY || end_of_request {
            {
                let mut stream_map = state.connections.lock().await;
                let stream = stream_map.entry(path.clone()).or_default();

                let writer = &mut stream.writer.write().await;
                writer.data.append(&mut buffer);
                buffer.clear();

                let chunked_transfer_end = chunked_encoding && payload.len() == 0;
                let nonchunked_request_end =
                    !chunked_encoding && (i + 1) * BYTES_PER_CHUNK >= iters;

                if chunked_transfer_end || nonchunked_request_end {
                    writer.ended = true;
                }

                manage_readers(stream).await;
            }
        }
    }
    if created {
        add_path_into_db(&state, &path).await;
        return Ok(StatusCode::CREATED);
    } else if chunked_encoding {
        return Ok(StatusCode::NO_CONTENT);
    } else {
        return Ok(StatusCode::NO_CONTENT);
    }
}

async fn return_stream(State(state): AppState, Path(path): Path<String>) -> impl IntoResponse {
    let stream_map = state.connections.lock().await;
    if let Some(stream) = stream_map.get(&path) {
        let cache_stream = CacheStream {
            cache: stream.writer.clone(),
            wakers: stream.readers.clone(),
            index: 0,
        };
        let body = Body::from_stream(cache_stream);

        return Ok((StatusCode::ACCEPTED, [(TRANSFER_ENCODING, "chunked")], body));
    } else {
        return Err((StatusCode::NOT_FOUND, "Requested stream ID was not found!"));
    }
}

async fn correct_list_query(body: String) -> Result<String, &'static str> {
    if let Some(first_char) = body.chars().next() {
        if first_char != '/' {
            return Err("Incorrect path (must start with '/')!!!");
        }
    } else {
        return Err("Incorrect path (empty)!!!");
    }
    if !body.ends_with("/*") {
        return Err("Incorrect path (must end with /*)!!!");
    }
    return Ok(body[..body.len() - 1].to_string());
}

async fn list_files(method: Method, State(state): AppState, body: String) -> impl IntoResponse {
    if method.as_str() == "LIST" {
        match correct_list_query(body).await {
            Err(e) => return e.to_owned(),
            Ok(query) => {
                let path_database = state.path_database.lock().await;
                let mut result = "".to_owned();
                if let Some(files) = path_database.get(&query) {
                    let mut iterator = files.iter().peekable();
                    while let Some(file) = iterator.next() {
                        result = result + &query + file;
                        if iterator.peek().is_some() {
                            result += "\n";
                        }
                    }
                }
                return result.to_owned();
            }
        }
    }
    return "Unknown method!".to_owned();
}

#[tokio::main]
async fn main() {
    assert!(BYTES_PER_CHUNK < WRITE_BUFFER_CAPACITY);
    let state = MyState::default();

    let app = Router::new()
        .route("/*path", put(accept_stream))
        .route("/*path", delete(delete_stream))
        .route("/*path", get(return_stream))
        .route_service("/list", list_files.with_state(state.clone()))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[derive(Default, Clone, Debug, FromRef)]
struct MyState {
    path_database: Arc<Mutex<HashMap<String, HashSet<String>>>>,
    connections: Arc<Mutex<HashMap<String, StreamCon>>>,
}

#[derive(Clone, Default, Debug)]
struct StreamCon {
    readers: Arc<std::sync::Mutex<Vec<Waker>>>,
    writer: Arc<RwLock<Writer>>,
}

#[derive(Clone, Default, Debug)]
struct Writer {
    ended: bool,
    data: Vec<Bytes>,
}

struct CacheStream {
    cache: Arc<RwLock<Writer>>,
    wakers: Arc<std::sync::Mutex<Vec<Waker>>>,
    index: usize,
}

impl Stream for CacheStream {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (result, move_index) = match self.cache.try_read() {
            Ok(cache) => {
                if cache.ended == true && self.index == cache.data.len() {
                    (Poll::Ready(None), false)
                } else if self.index == cache.data.len() {
                    self.wakers.lock().unwrap().push(cx.waker().clone());
                    (Poll::Pending, false)
                } else {
                    (Poll::Ready(Some(Ok(cache.data[self.index].clone()))), true)
                }
            }
            Err(_e) => (Poll::Pending, false),
        };
        if move_index {
            self.get_mut().index += 1;
        }
        return result;
    }
}
