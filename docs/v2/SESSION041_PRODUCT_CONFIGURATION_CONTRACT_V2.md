# S41 product run configuration contract — V2

Status: product-local configuration API and desktop editor. Configuration describes a non-capital research run and an owner-selected demo/test venue identity. It does not create an authenticated venue connection, read account state, submit/cancel orders, or enable live capital. Capital and assisted execution remain `false`; model/critic authority remains `ZERO`; economics remain `NOT ESTIMABLE`.

## Run identity

`ResearchRunConfigV2` is immutable before preflight and is included in the existing run manifest and `config_hash`. It records:

- canonical local `data_root`;
- a unique set of registered `public_venues` (`BYBIT`, `BINANCE`), allowing both;
- `selected_execution_venue` and `execution_environment` (`DEMO` or `TESTNET`);
- `product_scope=LINEAR_PERPETUAL`, `scope=BOUNDED_UNIVERSE_V2`, and `resource_profile_id=bounded-v2`;
- optional opaque account, capability-profile, and credential references;
- explicit execution profile and native demo/test symbol;
- optional canonical hash of an owner-supplied S8 research pair configuration;
- existing report interval, declared disk minimum, and intelligence provider profile.

Public collection selection and demo/test execution selection are independent: for example, public collection may use Bybit while the owner records Binance as the intended execution venue. Public data adapters are mainnet/public only; the execution environment is descriptive demo/test identity. These fields do not assert that each adapter, environment, product universe, account, or capability has been qualified. Source construction must resolve each exact immutable public venue to its registered adapter and refuse unsupported combinations before network setup; it must not substitute a venue or environment.

The desktop provides separate, explicit account-check and native demo/test OMS controls. Public collection does not start them. Account checks retrieve the exact protected credential after acquiring the single selected-account writer lease. Native OMS qualification has the same lease and immutable venue/environment/account/product identity. These controls do not grant opening, capital, or assisted authority. Both venue opening profiles remain fail-closed pending the required fill-time protection capability qualification.

The optional S8 selector accepts strict JSON declarations, copies their canonical content to `owner-pairs.json` before run startup, and binds its hash into `config_hash`. Source-file changes cannot change an existing run. Run loading rejects a changed, missing, symlinked, or unregistered snapshot. The sole evidence writer registers the exact owner catalog on first startup; restart retains its first publication time. Pairs remain research configuration with zero capital authority.

Opaque references accept a bounded `ref_…` alias or SHA-256 reference. Manifests, exports, UI messages, and diagnostics never contain API keys, API secrets, or raw account identifiers. A credential reference is only a pointer to an OS-protected record. Public collection remains usable without credentials.

## Credential storage

The desktop offers masked entry for a demo/test API key and secret. Windows DPAPI encrypts the pair under the selected data root's `secrets` directory, with the venue, environment, and opaque reference bound inside the encrypted record and the filename derived from their hash. The UI reports only protected-record presence and sanitized account-check results. There is no plaintext or cross-platform fallback. Authenticated calls require the separate, explicit selected demo/test account-check or OMS action.

The optional DeepSeek critic key remains a distinct protected credential. V2 runs resolve that existing key under their immutable data root, while V1 runs retain the legacy default data-root lookup. Provider selection remains explicit in the immutable run configuration and cannot change action, risk, or execution authority.

## Compatibility and bounds

V1 manifests retain their V1 config parser, content hash, default-root behavior, and exact-build resume rule. A V2 run must remain under its declared canonical data root; moving it makes the manifest invalid. Configuration edits require a new run identity.

The registered S40 queue/storage preflight thresholds remain unchanged. In particular, the tested 160 fps sustained / 320 fps burst workload, queue limits, capture bounds, storage reserve, strict export validation, and lifetime qualification latch are not enlarged by this config. The bounded universe identifier does not stand for a completed broad-market load test. Any new adapter, venue/environment combination, symbol scope, or workload needs its own bounded source and capacity evidence before it can pass a qualification gate.

Preflight remains a local storage gate bound to the immutable run/path. Operational GREEN indicates no current health warning; it does not promote source, endurance, account capability, execution, or economic qualification. RED latch evidence is immutable for the run lifetime; reconnect or changed display state cannot make the run valid again.
