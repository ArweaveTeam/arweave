## Schema

Path manifests are JSON objects with the following keys.

| Field            | Mandatory? | Type   | Description |
| ---------------- | ---------- | ------ | ----------- |
| `manifest`       | ✓          | string | The manifest type identifier, this MUST be `arweave/paths`. |
| `version`        | ✓          | string | The manifest specification version, currently "0.1.0". This will be updated with future updates according to [semver](https://semver.org). |
| `index`          |            | object | The behavior gateways SHOULD follow when the manifest is accessed directly. When defined, `index` MUST contain a member describing the behavior to adopt. Currently, the only supported behavior is `path`. `index` MAY be be omitted, in which case gateways SHOULD serve a listing of all paths. |
| `index.path`     |            | string | The default path to load. If defined, the field MUST reference a key in the `paths` object (it MUST NOT reference a transaction ID directly). |
| `paths`          | ✓          | object | The path mapping between subpaths and the content they resolve to. The object keys represent the subpaths, and the values tell us which content to resolve to. |
| `paths[path].id` | ✓          | string | The transaction ID to resolve to for the given path. |

A path manifest transaction MUST NOT contain any data other than this JSON object.

The `Content-Type` tag for manifest files MUST be `application/x.arweave-manifest+json`, users MAY add other arbitrary user defined tags.

**Example manifest**

```json
{
  "manifest": "arweave/paths",
  "version": "0.1.0",
  "index": {
    "path": "index.html"
  },
  "paths": {
    "index.html": {
      "id": "cG7Hdi_iTQPoEYgQJFqJ8NMpN4KoZ-vH_j7pG4iP7NI"
    },
    "js/style.css": {
      "id": "fZ4d7bkCAUiXSfo3zFsPiQvpLVKVtXUKB6kiLNt2XVQ"
    },
    "css/style.css": {
      "id": "fZ4d7bkCAUiXSfo3zFsPiQvpLVKVtXUKB6kiLNt2XVQ"
    },
    "css/mobile.css": {
      "id": "fZ4d7bkCAUiXSfo3zFsPiQvpLVKVtXUKB6kiLNt2XVQ"
    },
    "assets/img/logo.png": {
      "id": "QYWh-QsozsYu2wor0ZygI5Zoa_fRYFc8_X1RkYmw_fU"
    },
    "assets/img/icon.png": {
      "id": "0543SMRGYuGKTaqLzmpOyK4AxAB96Fra2guHzYxjRGo"
    }
  }
}
```
