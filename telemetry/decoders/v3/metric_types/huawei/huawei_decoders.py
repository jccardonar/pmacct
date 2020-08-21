from .huawei_metrics import GrpcRawGPBToHuaweiGrpcGPB, HuaweiGrpcGPBToHuaweiCompact, HuaweCompactToHuaweiElements

def huawei_compact_pipeline(huawei_decorator_constructor):
    return [GrpcRawGPBToHuaweiGrpcGPB(), HuaweiGrpcGPBToHuaweiCompact, HuaweCompactToHuaweiElements(huawei_decorator_constructor)]

