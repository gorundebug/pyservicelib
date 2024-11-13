#!/bin/bash
original_dir=$(pwd)
path=$(dirname "$0")
cd "$path" || { echo "Error: Failed to change directory to script location"; exit 1; }

#pip install pylint
pyreverse -p pyservicelib_gorundebug/ ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.appsink.AppSinkStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.map.MapStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.filter.FilterStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.flatmap.FlatMapStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.flatmapiterable.FlatMapIterableStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.foreach.ForEachStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.input.InputStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.join.JoinStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.keyby.KeyByStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.link.LinkStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.merge.MergeStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.multijoin.MultiJoinStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.multijoin.MultiJoinLinkStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.parallels.ParallelsStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.delay.DelayStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.sink.SinkStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.split.SplitStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.split.TypedBinarySplitStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.split.TypedBinaryKVSplitStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.stub.InStubStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.stub.InStubKVStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.stub.OutStubStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.stub.OutStubBinaryStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.stub.OutStubBinaryKVStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug

pyreverse -c pyservicelib_gorundebug.datasource.http.aiohttpds.AIOHttpEndpointConsumer \
 -c pyservicelib_gorundebug.datasource.http.aiohttpds.AIOHttpDataSource \
 -c pyservicelib_gorundebug.datasource.http.aiohttpds.AIOHttpEndpoint \
 -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug

pyreverse -c pyservicelib_gorundebug.datasource.localsource.custom.TypedCustomEndpointConsumer \
 -c pyservicelib_gorundebug.datasource.localsource.custom.CustomDataSource \
 -c pyservicelib_gorundebug.datasource.localsource.custom.CustomInputEndpoint \
 -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug

pyreverse -c pyservicelib_gorundebug.datasink.localsink.custom.TypedCustomEndpointConsumer \
 -c pyservicelib_gorundebug.datasink.localsink.custom.CustomDataSink \
 -c pyservicelib_gorundebug.datasink.localsink.custom.CustomSinkEndpoint \
 -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug

pyreverse -c pyservicelib_gorundebug.runtime.stub.OutStubBinaryKVStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug
pyreverse -c pyservicelib_gorundebug.runtime.stub.OutStubBinaryKVStream -p pyservicelib_gorundebug/ -d ./diagram ./src/pyservicelib_gorundebug



cd "$original_dir" || { echo "Error: Failed to change directory to '$original_dir'"; exit 1; }
