package io.github.eventjets.vertx.discovery;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import io.github.eventjets.vertx.nacos.VertxNacosClient;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.spi.ServiceDiscoveryBackend;
import io.vertx.servicediscovery.spi.ServiceType;
import io.vertx.servicediscovery.types.HttpEndpoint;

import java.util.*;

public class NacosBackendService implements ServiceDiscoveryBackend {

    private NamingService namingService;
    private Vertx vertx;
    private String nacosGroupName;
    private String nacosNamespace;

    @Override
    public void init(Vertx vertx, JsonObject config) {
        this.vertx = vertx;
        this.nacosGroupName = config.getJsonObject("nacos").getString("groupName");
        this.nacosNamespace = config.getJsonObject("nacos").getString("namespace");
        try {
            namingService = VertxNacosClient.createNamingService(config);
        } catch (NacosException e) {

        }
    }


    private String createId(Record record) {
        return new ServiceInfo(record.getName(), record.getLocation().getString("ip"), record.getLocation().getInteger("port")).getId();
    }


    @Override
    public void store(Record record, Handler<AsyncResult<Record>> resultHandler) {
        if (record.getRegistration() != null) {
            resultHandler.handle(Future.failedFuture("The record has already been registered"));
            return;
        }
        String id = createId(record);
        record.setRegistration(id);
        vertx.executeBlocking(promise -> {
            try {
                namingService.registerInstance(record.getName(), mapRecordToInstance(record));
                promise.complete();
            } catch (NacosException e) {
                promise.fail(e);
            }
        }, ar -> {
            if (ar.succeeded()) {
                resultHandler.handle(Future.succeededFuture(record));
            } else {
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private Instance mapRecordToInstance(Record record) {
        Instance instance;
        if (Objects.equals(record.getType(), HttpEndpoint.TYPE)) {
            instance = createHttpInstance(record);
        } else if (record.getType().equals(ServiceType.UNKNOWN)) {
            instance = createUnknownInstance(record);
        } else {
            throw new IllegalStateException("Not support record type: " + record);
        }

        return instance;
    }

    private Record mapInstanceToRecord(Instance instance) {
        Record record = new Record();

        record.setRegistration(instance.getInstanceId());
        record.setMetadata(JsonObject.mapFrom(instance.getMetadata()));
        record.setLocation(new JsonObject().put("host", instance.getIp()).put("port", instance.getPort()));
        record.setType(instance.getMetadata().get("_type"));

        return record;

    }

    private Instance createHttpInstance(Record record) {

        Instance instance = new Instance();
        instance.setInstanceId(record.getRegistration());
        instance.setServiceName(record.getName());
        instance.setIp(record.getLocation().getString("host"));
        instance.setPort(record.getLocation().getInteger("port"));
        instance.setEnabled(true);
        Map<String, Object> metadata = record.getMetadata().getMap();
        metadata.put("_type", "http");
        Map<String, String> stringMeta = Maps.transformEntries(metadata, new StringMapTransformer());
        instance.setMetadata(stringMeta);
        return instance;
    }

    private Instance createUnknownInstance(Record record) {
        Instance instance = new Instance();
        instance.setInstanceId(record.getRegistration());
        instance.setServiceName(record.getName());
        instance.setIp(record.getLocation().getString("host"));
        instance.setPort(record.getLocation().getInteger("port"));
        instance.setEnabled(true);
        Map<String, Object> metadata = record.getMetadata().getMap();
        Map<String, String> stringMeta = Maps.transformEntries(metadata, new StringMapTransformer());
        instance.setMetadata(stringMeta);
        return instance;
    }

    static class StringMapTransformer implements Maps.EntryTransformer<String, Object, String> {

        @Override
        public String transformEntry(String key, Object value) {
            if (value == null) {
                return "";
            }
            return value.toString();
        }
    }

    @Override
    public void remove(Record record, Handler<AsyncResult<Record>> resultHandler) {
        Objects.requireNonNull(record.getRegistration(), "No registration id in the record");
        remove(record.getRegistration(), resultHandler);
    }

    @Override
    public void remove(String uuid, Handler<AsyncResult<Record>> resultHandler) {
        vertx.executeBlocking(promise -> {
            ServiceInfo serviceInfo = new ServiceInfo(uuid);
            try {
                namingService.deregisterInstance(serviceInfo.getServiceName(), serviceInfo.getIp(), serviceInfo.getPort());
                promise.complete();
            } catch (NacosException e) {
                e.printStackTrace();
                promise.fail(e);
            }
        }, resultHandler);
    }

    @Override
    public void update(Record record, Handler<AsyncResult<Void>> resultHandler) {

    }

    @Override
    public void getRecords(Handler<AsyncResult<List<Record>>> resultHandler) {

    }


    @Override
    public void getRecord(String uuid, Handler<AsyncResult<Record>> resultHandler) {

        vertx.executeBlocking(promise -> {
            ServiceInfo serviceInfo = new ServiceInfo(uuid);
            try {
                Optional<Instance> svc = namingService.getAllInstances(serviceInfo.getServiceName(), this.nacosGroupName)
                        .stream().filter(sv -> Objects.equals(sv.getInstanceId(), uuid)).findFirst();
                if (svc.isPresent()) {
                    promise.complete(mapInstanceToRecord(svc.get()));
                } else {
                    promise.complete();
                }
            } catch (NacosException e) {
                e.printStackTrace();
                promise.fail(e);
            }
        }, resultHandler);
    }

    static class ServiceInfo {
        private final String serviceName;
        private final String ip;
        private final Integer port;

        public ServiceInfo(String id) {
            List<String> splits = Splitter.on(":").trimResults().splitToList(id);
            if (splits.size() == 3) {
                serviceName = splits.get(0);
                ip = splits.get(1);
                port = Integer.parseInt(splits.get(2));
            } else {
                throw new IllegalStateException("id is illegal format");
            }
        }


        public ServiceInfo(String serviceName, String ip, Integer port) {
            this.serviceName = serviceName;
            this.ip = ip;
            this.port = port;
        }

        public String getServiceName() {
            return serviceName;
        }

        public String getIp() {
            return ip;
        }

        public Integer getPort() {
            return port;
        }

        public String getId() {
            return serviceName + ":" + ip + ":" + port;
        }
    }
}
