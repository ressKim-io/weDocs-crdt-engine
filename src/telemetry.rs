//! OTel 트레이싱 부트스트랩 — W3C `traceparent` 전파(가드레일 4) + `tracing`→OTel 브리지.
//!
//! 익스포터는 `OTEL_TRACES_EXPORTER`로 선택: `otlp`(→ Collector/Jaeger, gRPC) | 그 외=stdout(기본).
//! 익스포터 구성이 실패해도 **panic하지 않고 콘솔 로깅만으로 degrade** — 트레이싱 부재가 서버
//! 기동을 막지 않게(M1 thin: trace는 showcase, 가용성보다 우선하지 않음).

use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

const SERVICE_NAME: &str = "wedocs-crdt-engine";

/// 트레이싱 초기화. 반환된 provider는 main이 보유하다가 종료 시 `shutdown()`으로 span flush.
/// 익스포터 구성 실패 시 `None`(OTel 없이 콘솔 로깅만) — 트레이싱 부재가 서버 기동을 막지 않게.
pub fn init() -> Option<SdkTracerProvider> {
    // 게이트웨이(Java javaagent)가 주입한 W3C traceparent를 추출/주입하는 전역 propagator.
    global::set_text_map_propagator(TraceContextPropagator::new());

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // 익스포터를 먼저 구성(실패해도 degrade). 결과 로깅은 subscriber init 이후로 미룬다 —
    // tracing 매크로는 subscriber가 설치되기 전엔 무음이므로.
    let outcome = build_provider();
    let registry = tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer());
    match &outcome {
        Ok((provider, _)) => {
            let tracer = provider.tracer(SERVICE_NAME);
            registry
                .with(tracing_opentelemetry::layer().with_tracer(tracer))
                .init();
        }
        Err(_) => registry.init(),
    }

    match outcome {
        Ok((provider, description)) => {
            // 샘플러는 기본값 parentbased_always_on — 게이트웨이 traceparent의 sampled 플래그를
            // 상속해 2-hop을 한 trace로 잇는다(엔진이 독립 샘플링하지 않음).
            tracing::info!(exporter = %description, "OTel 트레이싱 활성");
            Some(provider)
        }
        Err(error) => {
            tracing::warn!(error = %error, "OTel 익스포터 구성 실패 → 콘솔 로깅만(trace 전파 비활성)");
            None
        }
    }
}

/// `OTEL_TRACES_EXPORTER`에 따라 tracer provider를 구성. otlp=batch(gRPC), 그 외=stdout(simple).
/// 두 번째 반환값은 시작 로그용 익스포터 설명(운영 시 어디로 보내는지 명확히).
fn build_provider() -> Result<(SdkTracerProvider, String), Box<dyn std::error::Error>> {
    let resource = Resource::builder().with_service_name(SERVICE_NAME).build();
    let builder = SdkTracerProvider::builder().with_resource(resource);

    let kind = std::env::var("OTEL_TRACES_EXPORTER").unwrap_or_else(|_| "stdout".to_string());
    let result = match kind.as_str() {
        "otlp" => {
            // gRPC 기본 endpoint = http://localhost:4317. `OTEL_EXPORTER_OTLP_ENDPOINT`로 override.
            // 주의: gRPC는 `/v1/traces` path를 붙이지 않는다 — endpoint에 path를 넣으면 연결 실패.
            let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
                .unwrap_or_else(|_| "http://localhost:4317".to_string());
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .build()?;
            let provider = builder.with_batch_exporter(exporter).build();
            (provider, format!("otlp/grpc → {endpoint}"))
        }
        // stdout: simple 익스포터 — span을 콘솔에 동기 출력(docker-free trace_id 일치 확인용).
        _ => {
            let provider = builder
                .with_simple_exporter(opentelemetry_stdout::SpanExporter::default())
                .build();
            (provider, "stdout".to_string())
        }
    };
    Ok(result)
}
