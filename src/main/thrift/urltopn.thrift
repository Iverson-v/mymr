namespace java com.ksc.urltopn.thrift

struct UrlTopNAppRequest {
    1: string applicationId;
    2: string inputPath;
    3: string ouputPath;
    4: i32 topN;
    5: i32 numReduceTasks;
    6: i32 splitSize;
}

struct UrlTopNAppResponse {
    1: string applicationId;
    2: i32 appStatus; // 0: accepted, 1: running, 2: finished, 3: failed
}

struct UrlTopNResult {
    1: string url;
    2: i32 count;
}

service UrlTopNService {
    UrlTopNAppResponse submitApp(1: UrlTopNAppRequest urlTopNAppRequest),
    UrlTopNAppResponse getAppStatus(1: string applicationId),
    list<UrlTopNResult> getTopNAppResult(1: string applicationId)
}