module github.com/rafet/gobreaker-redis/benchmarks

go 1.26.2

require (
	github.com/cep21/circuit/v4 v4.1.0
	github.com/exaring/hoglet v0.3.1
	github.com/failsafe-go/failsafe-go v0.9.6
	github.com/mercari/go-circuitbreaker v0.0.2
	github.com/rafet/gobreaker-redis/v2 v2.0.0-rc.2
	github.com/rubyist/circuitbreaker v2.2.1+incompatible
	github.com/sony/gobreaker v1.0.0
	github.com/sony/gobreaker/v2 v2.4.0
)

require (
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/bits-and-blooms/bitset v1.24.4 // indirect
	github.com/cenk/backoff v2.2.1+incompatible // indirect
	github.com/cenkalti/backoff/v3 v3.1.1 // indirect
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a // indirect
	github.com/peterbourgon/g2s v0.0.0-20170223122336-d4e7ad98afea // indirect
	golang.org/x/sync v0.18.0 // indirect
)

replace github.com/rafet/gobreaker-redis/v2 => ..
