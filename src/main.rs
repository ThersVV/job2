use axum::extract::Path;
use axum::Json;
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{header::TRANSFER_ENCODING, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Error, Router,
};
use tokio::time::{sleep, Duration};

use core::pin::Pin;
use futures_core::task::Waker;
use futures_core::{
    task::{Context, Poll},
    Stream,
};
use serde::Serialize;
use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex, RwLock};
type AppState = State<MyState>;

const BYTES_PER_CHUNK: usize = 8;

async fn hello_world<'a>() -> &'a str {
    return "hi!";
}

async fn placeholder_post(
    State(state): AppState,
    Path(id): Path<usize>,
    headers: HeaderMap,
    payload: Bytes,
) -> impl IntoResponse {
    println!("start");
    let chunked_encoding =
        headers.contains_key(TRANSFER_ENCODING) && headers[TRANSFER_ENCODING] == "chunked";
    if !(true/* chunked_encoding */) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Transfer encoding is not chunked!".to_owned(),
        ));
    }

    let mut buffer = Vec::new();
    let mut i = 0;
    let iters = payload.len();

    {
        let mut stream_map = state.connections.lock().unwrap();
        let stream = stream_map.deref_mut().entry(id).or_default();
    }
    while i * BYTES_PER_CHUNK < iters {
        // tohle musí jít líp
        let (payload_start, payload_end) = (
            i * BYTES_PER_CHUNK,
            std::cmp::min((i + 1) * BYTES_PER_CHUNK, iters),
        );
        buffer.push(Bytes::copy_from_slice(&payload[payload_start..payload_end]));

        if buffer.len() > 10000 || !((i + 1) * BYTES_PER_CHUNK < iters) {
            {
                let mut stream_map = state.connections.lock().unwrap();
                let stream = stream_map.entry(id).or_default();

                let writer = &mut stream.writer.write().unwrap();
                writer.data.append(&mut buffer);
                buffer.clear();

                if payload.len() == 0 || (!chunked_encoding && !((i + 1) * BYTES_PER_CHUNK < iters))
                {
                    writer.ended = true;
                }

                let readers = &mut stream.readers.lock().unwrap();
                for reader in readers.iter() {
                    reader.clone().wake();
                }
                readers.clear();
            }
            println!("mid");
            sleep(Duration::from_millis(1000)).await;
        }

        i += 1;
    }
    println!("end");
    return Ok((StatusCode::ACCEPTED, "Chunk finished!".to_owned()));
}

async fn placeholder_get(State(state): AppState, Path(id): Path<usize>) -> impl IntoResponse {
    let stream_map = state.connections.lock().unwrap();
    println!("{:?}, {:p}", stream_map.len(), state.connections);
    if let Some(stream) = stream_map.get(&id) {
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

#[tokio::main]
async fn main() {
    let state = MyState::default();

    let app = Router::new()
        .route("/", get(hello_world))
        .route("/upload/:id", post(placeholder_post))
        .route("/download/:id", get(placeholder_get))
        .with_state(state);
    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[derive(Default, Clone, Debug)]
struct MyState {
    connections: Arc<Mutex<HashMap<usize, StreamCon>>>,
}

#[derive(Clone, Default, Debug)]
struct StreamCon {
    readers: Arc<Mutex<Vec<Waker>>>,
    writer: Arc<RwLock<Writer>>,
}

#[derive(Clone, Default, Debug)]
struct Writer {
    ended: bool,
    data: Vec<Bytes>,
}

struct CacheStream {
    cache: Arc<RwLock<Writer>>,
    wakers: Arc<Mutex<Vec<Waker>>>,
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
