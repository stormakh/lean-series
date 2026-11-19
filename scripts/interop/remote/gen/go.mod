module prometheus-rs/interop/remote/gen

go 1.26.7

require (
	github.com/golang/snappy v1.0.0
	github.com/prometheus/prometheus v0.0.0-00010101000000-000000000000
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.72.1 // indirect
	golang.org/x/text v0.43.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/prometheus/prometheus => /home/kamrot/code/prometheus-rs/upstream
