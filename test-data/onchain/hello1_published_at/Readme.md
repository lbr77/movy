# Hello1 with published-at


The package was deployed at bcb4367516cc4f22b7c0265db04e9b4e83b1f24b0b0455020f26e448df6fd08 firstly and then upgraded to 0x18f8f4ebada51361c6558c7cfa77cb46be72fca7caacc475dc828858becfaa75 with a new module hello1_upgrade.

There is a `published-at` added by design.

Please note the address alias:

```
[addresses]
hello1 = "0x0bcb4367516cc4f22b7c0265db04e9b4e83b1f24b0b0455020f26e448df6fd08"
```

if we wrongly set it to:

```
[addresses]
hello1 = "0x18f8f4ebada51361c6558c7cfa77cb46be72fca7caacc475dc828858becfaa75"
```

the deployment of `hello5` will fail because it should be the original id.