# VGI C# documentation examples

This project is the canonical source for the examples embedded in the C# documentation at
query.farm. It deliberately covers each primary worker function shape plus catalog registration.

Build and test it from the repository root:

```sh
make docs_examples
make test_docs_examples
```

`verify.sh` builds the worker and runs the SQLLogicTest file through `uvx haybarn-unittest`.
