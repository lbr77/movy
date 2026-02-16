# Hello1


The package was deployed at bcb4367516cc4f22b7c0265db04e9b4e83b1f24b0b0455020f26e448df6fd08 firstly and then upgraded to 0x18f8f4ebada51361c6558c7cfa77cb46be72fca7caacc475dc828858becfaa75 with a new module hello1_upgrade.

Published-at field is added for compatibility.

## Some notes

On Sui, a package's named address must always be its original (v1) package ID, not the upgraded version's ID. The `published-at` field correctly points to the latest version (for linking), but the address must remain the original.

Each package object has a `linkage_table: BTreeMap<ObjectID, UpgradeInfo>,` that maps the original package address to the (possibly upgraded) actual used package address. 


AI Explanation:

Yes, exactly — the `linkage_table` is at the heart of it. Let me break down how it works:

### Sui's On-Chain Package Model

Every on-chain package object has a structure roughly like:

```
Package {
    id: ObjectID,                                    // this package's own ID
    modules: BTreeMap<String, CompiledModule>,
    linkage_table: BTreeMap<ObjectID, UpgradeInfo>,  // key = original ID
}
```

The `linkage_table` maps **original package IDs → `UpgradeInfo`** (which contains the upgraded/actual version ID to use at runtime). This is how Sui resolves dependencies when a depended-on package has been upgraded.

### Why the named address must be the original ID

When the Move compiler builds your bytecode, **the named address (`hello1 = "0x..."`) gets baked into the compiled module as the package address**. Every cross-module reference in the bytecode uses this address — e.g., when `hello2` calls `hello1::hello1_upgrade::hello1_upgrade()`, the bytecode literally encodes a reference to the address from `[addresses] hello1`.

At publish/execution time, Sui's runtime does a lookup in the **calling package's `linkage_table`** to resolve which actual on-chain package to load:

1. It sees bytecode referencing address `0x0bcb...` (the original ID)
2. It looks up `0x0bcb...` in the `linkage_table`
3. The `linkage_table` entry says: "use version at `0x18f8...`"
4. It loads the modules from `0x18f8...`

If you set the named address to the **upgraded** ID (`0x18f8...`) instead:

1. The bytecode references `0x18f8...`
2. Sui looks up `0x18f8...` in the `linkage_table`
3. **No entry found** — because the key is always the *original* ID (`0x0bcb...`)
4. → `PublishUpgradeMissingDependency`

### The role of `published-at`

`published-at` serves a **different purpose** — it tells the **Move linker** (at build time) which on-chain package version to link against, so it can verify that the modules and functions you're calling actually exist in that version. It does **not** affect the address baked into bytecode.

So in summary:

| Concept | Value | Used by | Purpose |
|---|---|---|---|
| `[addresses] hello1` | Original ID (`0x0bcb...`) | Compiler → bytecode | Baked into bytecode; used as `linkage_table` lookup key at runtime |
| `published-at` | Latest version ID (`0x18f8...`) | Build-time linker | Verifies the dependency's modules/functions exist on-chain |
| `linkage_table` key | Original ID (`0x0bcb...`) | Runtime | Maps original → upgraded version for actual code loading |

The invariant is: **bytecode addresses must match `linkage_table` keys, which are always original package IDs**. That's why the named address must never change across upgrades.