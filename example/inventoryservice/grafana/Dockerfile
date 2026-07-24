FROM golang:1.25-alpine AS jsonnet_builder

RUN GOBIN=/out go install github.com/google/go-jsonnet/cmd/jsonnet@v0.20.0

FROM alpine:3.22

RUN apk add --no-cache curl tar
COPY --from=jsonnet_builder /out/jsonnet /usr/local/bin/jsonnet

RUN mkdir -p /vendor/github.com/grafana/grafonnet/gen/grafonnet-v11.0.0 && \
    curl -fsSL https://github.com/grafana/grafonnet/archive/refs/heads/main.tar.gz \
    | tar xz --strip-components=3 \
        -C /vendor/github.com/grafana/grafonnet/gen/grafonnet-v11.0.0 \
        grafonnet-main/gen/grafonnet-v11.0.0

ARG SERVICE_NAME
COPY --from=dashboard_sources . /dashboards/
WORKDIR /dashboards

RUN mkdir -p /dist && \
    for src in /dashboards/[0-9]*.jsonnet; do \
        out="/dist/$(basename "${src%.jsonnet}.json")"; \
        jsonnet -J /vendor --ext-str service="${SERVICE_NAME}" "$src" -o "$out"; \
    done

CMD cp /dist/*.json /output/
