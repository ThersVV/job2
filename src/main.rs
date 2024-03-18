use axum::{
    body::{Body, Bytes},
    debug_handler,
    extract::{FromRef, Path, State},
    handler::Handler,
    http::{header::TRANSFER_ENCODING, HeaderMap, Method, StatusCode},
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

async fn hello_world<'a>() -> &'a str {
    return "hi!";
}

async fn delete_stream(State(state): AppState, Path(path): Path<String>) -> impl IntoResponse {
    let mut stream_map = state.connections.lock().await;
    let valid_id = stream_map.contains_key(&path);
    if valid_id {
        stream_map.remove(&path);
        delete_path_from_db(&state, &path).await;
        return Ok((StatusCode::NO_CONTENT, "File succesfully deleted!"));
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

async fn delete_path_from_db(state: &MyState, path: &str) {
    // consider function that returns key-value pairs
    let mut path_database = state.path_database.lock().await;
    let mut key_buffer = "".to_string();
    let exploded_path = path.split('/');
    let mut key_iterator = exploded_path.clone().enumerate().peekable();

    while let Some((i, key_word)) = key_iterator.next() {
        key_buffer = key_buffer + key_word;
        if key_iterator.peek().is_some() {
            key_buffer = key_buffer + "/";
            let key_entry = path_database.entry(key_buffer.to_string()).or_default();

            let mut value_buffer = "".to_string();
            let mut value_iterator = exploded_path.clone().skip(i + 1).peekable();
            while let Some(value_word) = value_iterator.next() {
                value_buffer = value_buffer + value_word;
                if value_iterator.peek().is_some() {
                    value_buffer = value_buffer + "/";
                }
            }

            if key_entry.len() < 2 {
                path_database.remove(&key_buffer);
            } else {
                key_entry.remove(&value_buffer);
            }
        } else {
            let key_entry = path_database.entry(key_buffer.to_string()).or_default();
            if key_entry.len() < 2 {
                path_database.remove(&key_buffer);
            } else {
                key_entry.remove("");
            }
        }
    }
    println!("{:?}", path_database);
}

async fn add_path_into_db(state: &MyState, path: &str) {
    let mut path_database = state.path_database.lock().await;
    let mut key_buffer = "".to_string();
    let exploded_path = path.split('/');
    let mut key_iterator = exploded_path.clone().enumerate().peekable();

    while let Some((i, key_word)) = key_iterator.next() {
        key_buffer = key_buffer + key_word;
        if key_iterator.peek().is_some() {
            key_buffer = key_buffer + "/";
            let key_entry = path_database.entry(key_buffer.to_string()).or_default();

            let mut value_buffer = "".to_string();
            let mut value_iterator = exploded_path.clone().skip(i + 1).peekable();
            while let Some(value_word) = value_iterator.next() {
                value_buffer = value_buffer + value_word;
                if value_iterator.peek().is_some() {
                    value_buffer = value_buffer + "/";
                }
            }
            key_entry.insert(value_buffer.to_string());
        } else {
            let key_entry = path_database.entry(key_buffer.to_string()).or_default();
            key_entry.insert("".to_string());
        }
    }
    println!("{:?}", path_database);
}

async fn accept_stream(
    State(state): AppState,
    Path(path): Path<String>,
    headers: HeaderMap,
    payload: Bytes,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    //check for empty put request!
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

        //measure time, maybe Bytes is smart enough to not copy to writer and just reference it (should be)
        buffer.push(Bytes::copy_from_slice(&payload[payload_start..payload_end]));

        let end_of_request = (i + 1) * BYTES_PER_CHUNK >= iters;
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
        return Ok((StatusCode::CREATED, "Resource has been created!".to_owned()));
    } else if chunked_encoding {
        return Ok((StatusCode::NO_CONTENT, "Chunk finished!".to_owned()));
    } else {
        return Ok((StatusCode::NO_CONTENT, "Transfer finished!".to_owned()));
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

        return Ok((StatusCode::ACCEPTED, body));
    } else {
        return Err((StatusCode::NOT_FOUND, "Requested stream ID was not found!"));
    }
}

async fn list_files(
    method: Method,
    headers: HeaderMap,
    State(state): AppState,
    body: String,
) -> impl IntoResponse {
    /*
    TODO:
    Check if this alg is ok:
    just strip the star away from start and hash it
    when searching, try hash, if not working, strip all before dot and hash, repeat.
    when star is at the end, also just dump it ig, nginx did something like moving the end to the start but idk.
    you have to append an empty char to the end that cannot be extended!
    */
    if method.as_str() == "LIST" {
        return "Here is so many items oh my god";
    }
    return "Nope, nothing here!";
}

#[tokio::main]
async fn main() {
    assert!(BYTES_PER_CHUNK < WRITE_BUFFER_CAPACITY);
    let state = MyState::default();

    let app = Router::new()
        .route("/", get(hello_world))
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
