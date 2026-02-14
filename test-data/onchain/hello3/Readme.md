# hello3


This package depends on hello2(published already) and hello4(unpublished).

`sui client publish --dry-run` will give:

```
Failed to publish the Move module(s), reason: Package dependency "hello4" does not specify a published address (the Move.toml manifest for "hello4" does not contain a 'published-at' field, nor is there a 'published-id' in the Move.lock).
If this is intentional, you may use the --with-unpublished-dependencies flag to continue publishing these dependencies as part of your package (they won't be linked against existing packages on-chain).
```

so we do `sui client publish --with-unpublished-dependencies` instead and publish to 0xb85b49f4e129b9b97d556c9853978b3695d588c8068accd8285d5a9a4078f610

Do note `hello4` is bundled as well.