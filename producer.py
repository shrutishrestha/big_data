from kafka import KafkaProducer
import json
import time
import re
from big_data import *

kafka_topic = "registered_item"
if type(kafka_topic) == bytes:
    kafka_topic = kafka_topic.decode('utf-8')


producer_obj = KafkaProducer(bootstrap_servers = ['localhost:9092'], api_version=(0,11,5),value_serializer=lambda data: json.dumps(data).encode('utf-8'))


def registered_item(id, rdd):
    patch_size = PatchSize.FOUR
    mean_obj = MeanParamCalculator(patch_size)

    std_dev_obj = StdDeviationParamCalculator(patch_size)

    
#     skewness_obj = SkewnessParamCalculator(patch_size)

    
#     kurtosis_obj = KurtosisParamCalculator(patch_size)

    
#     relative_smoothness_obj = RelativeSmoothnessParamCalculator(patch_size)

    
#     uniformity_obj = UniformityParamCalculator(patch_size, n_bins = 5, min_val=0, max_val = 1)

    
#     entropy_obj = EntropyParamCalculator(patch_size,n_bins = 5, min_val=0, max_val = 1)

    
#     tcontrast_obj = TContrastParamCalculator(patch_size)
    
#     collection_rdd_fake = rdd.map(mean_obj.calculate_parameter)

    item_obj = {
        "id": id,
        "mean": rdd.map(mean_obj.calculate_parameter).collect(), 
        "std":rdd.map(std_dev_obj.calculate_parameter).collect()
        # "skewness":rdd.map(skewness_obj.calculate_parameter).collect(),
        # "kurtosis":rdd.map(kurtosis_obj.calculate_parameter).collect(),
        # "relative_smoothness":rdd.map(relative_smoothness_obj.calculate_parameter).collect(),
        # "uniformity":rdd.map(uniformity_obj.calculate_parameter).collect(),
        # "entropy":rdd.map(entropy_obj.calculate_parameter).collect(),
        # "contrast":rdd.map(tcontrast_obj.calculate_parameter).collect()
    }
    return item_obj

if __name__ == "__main__":

    pysc = start()
    jp2s = ["/Users/shruti/big_data/2020_01_01__00_00_35_12__SDO_AIA_AIA_94.jp2"]
    arrs = []

    for jp2 in jp2s:
        with rasterio.open(jp2) as f:
            arrs.append(f.read(1))

    data = np.array(arrs, dtype=arrs[0].dtype)
    print("data", data)

    rdd = pysc.parallelize(data)
    
    

    if rdd!=None:
        item = registered_item(1, rdd)
        print("item",item)
        producer_obj.send(kafka_topic, item)
        producer_obj.flush()
        time.sleep(0.8)

    print("Kafka Producer Application Completed ... ")

