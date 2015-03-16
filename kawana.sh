#!/bin/bash
set -o errexit -o pipefail

flags=( "$@" )

if [ ! -z "$KAWANA_DATADIR" ]
then
    flags+=( -dataDir $KAWANA_DATADIR )
fi

if [ ! -z "$KAWANA_PERSIST" ]
then
    flags+=( -persist $KAWANA_PERSIST )
fi

if [ ! -z "$KAWANA_BACKUP" ]
then
    flags+=( -backup $KAWANA_BACKUP )
fi

if [ ! -z "$KAWANA_S3BUCKET" ]
then
    flags+=( -s3Bucket $KAWANA_S3BUCKET )
fi

if [ ! -z "$KAWANA_PORT" ]
then
    flags+=( -port $KAWANA_PORT )
fi

if [ ! -z "$KAWANA_PROCS" ]
then
    flags+=( -procs $KAWANA_PROCS )
fi

exec /go/bin/kawana "${flags[@]}"
