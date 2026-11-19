module prometheus-rs/scripts/gen/textparse

go 1.26.7

require (
	github.com/gogo/protobuf v1.3.2
	github.com/prometheus/prometheus v0.0.0-00010101000000-000000000000
)

require (
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.72.1 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/prometheus/prometheus => ../../../upstream
