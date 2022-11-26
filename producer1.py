from kafka import KafkaProducer
import json
import time
import re
from big_data import *

# kafka_topic = "registered_item"
# if type(kafka_topic) == bytes:
#     kafka_topic = kafka_topic.decode('utf-8')


producer_obj = KafkaProducer(bootstrap_servers = ['localhost:9092'], api_version=(0,11,5),value_serializer=lambda data: json.dumps(data).encode('utf-8'))
def register_item(data):
    item_obj={
        "id": str(1),
        "mean": str(data),
    }

    print("item_obj",item_obj)
    return item_obj

def f(data): 
    patch_size = PatchSize.FULL
    mean_obj = MeanParamCalculator(patch_size)
    skew_obj = SkewnessParamCalculator(patch_size)
    kurtosis_obj = KurtosisParamCalculator(patch_size)
    relative_smoothness_obj = RelativeSmoothnessParamCalculator(patch_size)
    uniformity_obj = UniformityParamCalculator(patch_size, n_bins = 5, min_val=0, max_val = 1)
    entropy_obj = EntropyParamCalculator(patch_size,n_bins = 5, min_val=0, max_val = 1)
    tcontrast_obj = TContrastParamCalculator(patch_size)

    mean_value = str(mean_obj.calculate_parameter(data))
    skew_value =  str(skew_obj.calculate_parameter(data))
    kurtosis_value =  str(kurtosis_obj.calculate_parameter(data))
    relative_smoothness_value =  str(relative_smoothness_obj.calculate_parameter(data))
    uniformity_value =  str(uniformity_obj.calculate_parameter(data))
    entropy_value =  str(entropy_obj.calculate_parameter(data))
    tcontrast_value =  str(tcontrast_obj.calculate_parameter(data))

    item_obj = {
        "id": 1,
        "mean": mean_value,
        "skew":skew_value,
        "kurtosis_value":kurtosis_value,
        "relative_smoothness_value":relative_smoothness_value,
        "uniformity_value":uniformity_value,
        "entropy_value":entropy_value,
        "tcontrast_value":tcontrast_value}
    print("item_obj",item_obj)
    return item_obj




if __name__ == "__main__":

    pysc = start()
    jp2s = ["/Users/shruti/big_data/2020_01_01__00_00_35_12__SDO_AIA_AIA_94.jp2"]
    arrs = []

    patch_size = PatchSize.FULL
    mean_obj = MeanParamCalculator(patch_size)

    for jp2 in jp2s:

        data = cv2.imread(jp2)
        data = data.transpose(2, 0, 1)
        data = np.expand_dims(data[0], axis=0)
        # arrs.append(data)
        rdd = pysc.parallelize(data)
        # item = rdd.foreach(f) 
        words_map = rdd.map(lambda data: mean_obj.calculate_parameter(data))
        mapping = words_map.collect()
        item_obj = register_item(mapping)    


        producer_obj.send("registered_user", item_obj)
        # producer_obj.flush()
        time.sleep(0.8)
    

    print("Kafka Producer Application Completed ... ")
