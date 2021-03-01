# go-storage-information

This tool was build to get quickly and easy Information about Azure Blobs.
It prints basic attributes and gets metadata from blobs.
It is also possible to download the content of blobs.
Furthermore it is possible to filter for a certain Storage Container Names or/and Blob names.

## Parameter

* `--accountName` or `-n`: (mandatory) provide the name of the Storage Account
* `--accessKey` or `-k`: (mandatory) provide the Access Key of Storage Account
* `--container` or `-c`: (optional) filter for a specific container by it names. Substring match
* `--blob` or `-b`: (optional) filter for specific blobs by it names. Substring match
* `--metadata-filter` or `-m`: (optional) filter for metadata <key:value>. Shows only the blobs matching at least one given filter.
* `--show-content`: (optional) prints additionally the content of the blobs

## Examples
* `./asi --accountName <myStorageAccountName> --accessKey <myStorageAccessKey> -c test -b myblob` will show only blobs including myblob in their names only stored in containers including test in their names.


* `./asi --accountName <myStorageAccountName> --accessKey <myStorageAccessKey> -m trackingId:123 -m foo:bar` will show only blobs having `trackingId = 123` or `foo = bar` as metadata properties.
