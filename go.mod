module go.sia.tech/hostd

go 1.19

require (
	github.com/aws/aws-sdk-go v1.44.156
	github.com/mattn/go-sqlite3 v1.14.16
	gitlab.com/NebulousLabs/encoding v0.0.0-20200604091946-456c3dc907fe
	gitlab.com/NebulousLabs/fastrand v0.0.0-20181126182046-603482d69e40
	gitlab.com/NebulousLabs/log v0.0.0-20210609172545-77f6775350e2
	gitlab.com/NebulousLabs/siamux v0.0.2-0.20220630142132-142a1443a259
	go.sia.tech/mux v1.1.1-0.20230119180453-05591decec67
	go.sia.tech/renterd v0.0.0-20221212140411-1e971169c463 // careful when upgrading -- renterd defines a "testing" sector size hostd does not know how to handle.
	go.sia.tech/siad v1.5.10-0.20230124165802-3bb7da1814db
	golang.org/x/crypto v0.5.0
	golang.org/x/sys v0.4.0
	golang.org/x/term v0.4.0
	golang.org/x/time v0.3.0
	lukechampine.com/frand v1.4.2
)

require (
	filippo.io/edwards25519 v1.0.0-rc.1 // indirect
	github.com/aead/chacha20 v0.0.0-20180709150244-8b13a72661da // indirect
	github.com/dchest/threefish v0.0.0-20120919164726-3ecf4c494abf // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/hdevalence/ed25519consensus v0.0.0-20220222234857-c00d1f31bab3 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.2 // indirect
	github.com/klauspost/reedsolomon v1.11.3 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	gitlab.com/NebulousLabs/bolt v1.4.4 // indirect
	gitlab.com/NebulousLabs/demotemutex v0.0.0-20151003192217-235395f71c40 // indirect
	gitlab.com/NebulousLabs/entropy-mnemonics v0.0.0-20181018051301-7532f67e3500 // indirect
	gitlab.com/NebulousLabs/errors v0.0.0-20200929122200-06c536cf6975 // indirect
	gitlab.com/NebulousLabs/go-upnp v0.0.0-20211002182029-11da932010b6 // indirect
	gitlab.com/NebulousLabs/merkletree v0.0.0-20200118113624-07fbf710afc4 // indirect
	gitlab.com/NebulousLabs/monitor v0.0.0-20191205095550-2b0fd3e1012a // indirect
	gitlab.com/NebulousLabs/persist v0.0.0-20200605115618-007e5e23d877 // indirect
	gitlab.com/NebulousLabs/ratelimit v0.0.0-20200811080431-99b8f0768b2e // indirect
	gitlab.com/NebulousLabs/threadgroup v0.0.0-20200608151952-38921fbef213 // indirect
	golang.org/x/net v0.5.0 // indirect
	golang.org/x/text v0.6.0 // indirect
)
