from kafka import KafkaProducer
import json
import time
import datetime
import re
from big_data import *

producer_obj = KafkaProducer(bootstrap_servers = ['localhost:9092'], api_version=(0,11,5),value_serializer=lambda data: json.dumps(data).encode('utf-8'))

def register_item(filename, rdd): 
    patch_size = PatchSize.FULL
    mean_obj = MeanParamCalculator(patch_size)
    skew_obj = SkewnessParamCalculator(patch_size)
    kurtosis_obj = KurtosisParamCalculator(patch_size)
    relative_smoothness_obj = RelativeSmoothnessParamCalculator(patch_size)
    uniformity_obj = UniformityParamCalculator(patch_size, n_bins = 5, min_val=0, max_val = 1)
    entropy_obj = EntropyParamCalculator(patch_size,n_bins = 5, min_val=0, max_val = 1)
    tcontrast_obj = TContrastParamCalculator(patch_size)
    grad_calc_obj = GradientCalculator('prewitt')


    mean_map = rdd.map(lambda data: mean_obj.calculate_parameter(data))
    skew_map = rdd.map(lambda data: skew_obj.calculate_parameter(data))
    kurtosis_map = rdd.map(lambda data: kurtosis_obj.calculate_parameter(data))
    relative_map = rdd.map(lambda data: relative_smoothness_obj.calculate_parameter(data))
    uniformity_map = rdd.map(lambda data: uniformity_obj.calculate_parameter(data))
    entropy_map = rdd.map(lambda data: entropy_obj.calculate_parameter(data))
    tcontrast_map = rdd.map(lambda data: tcontrast_obj.calculate_parameter(data))


    mean_value = mean_map.collect()
    skew_value = skew_map.collect()
    kurtosis_value = kurtosis_map.collect()
    relative_value = relative_map.collect()
    uniformity_value = uniformity_map.collect()
    entropy_value = entropy_map.collect()
    tcontrast_value = tcontrast_map.collect()


    item_obj = {
        "filename": filename,
        "mean": str(round(mean_value[0],6)),
        "skew":str(round(skew_value[0],6)),
        "kurtosis":str(round(kurtosis_value[0],6)),
        "relative_smoothness":str(round(relative_value[0],6)),
        "uniformity":str(round(uniformity_value[0],6)),
        "entropy":str(round(entropy_value[0],6)),
        "tcontrast":str(round(tcontrast_value[0],6)),
        }
    print("item_obj",item_obj)
    return item_obj




if __name__ == "__main__":

    pysc = start()
    jp2s =["/Users/shruti/big_data/jp2_images/2010_06_02__00_05_54_065__SDO_AIA_AIA_1600.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_05_30_045__SDO_AIA_AIA_1600.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_06_18_065__SDO_AIA_AIA_1600.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_06_16_565__SDO_AIA_AIA_335.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_06_04_565__SDO_AIA_AIA_335.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_05_40_545__SDO_AIA_AIA_335.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_05_28_545__SDO_AIA_AIA_335.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_06_51_075__SDO_AIA_AIA_304.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_06_03_065__SDO_AIA_AIA_304.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_05_51_055__SDO_AIA_AIA_304.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_05_39_065__SDO_AIA_AIA_304.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_06_01_585__SDO_AIA_AIA_211.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_05_49_555__SDO_AIA_AIA_211.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_05_37_565__SDO_AIA_AIA_211.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_06_19_575__SDO_AIA_AIA_193.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_06_07_555__SDO_AIA_AIA_193.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_05_55_585__SDO_AIA_AIA_193.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_05_43_585__SDO_AIA_AIA_193.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_05_31_555__SDO_AIA_AIA_193.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_06_24_065__SDO_AIA_AIA_171.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_06_00_065__SDO_AIA_AIA_171.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_05_48_055__SDO_AIA_AIA_171.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_05_36_065__SDO_AIA_AIA_171.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_06_22_595__SDO_AIA_AIA_131.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_05_58_595__SDO_AIA_AIA_131.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_05_46_545__SDO_AIA_AIA_131.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_05_34_565__SDO_AIA_AIA_131.jp2",
"/Users/shruti/big_data/jp2_images/2010_06_02__00_05_33_055__SDO_AIA_AIA_94.jp2"]
    arrs = []



    for jp2 in jp2s:
        start = datetime.now()

        filename = jp2.split("/")[-1]
        data = cv2.imread(jp2)
        data = data.transpose(2, 0, 1)
        data = np.expand_dims(data[0], axis=0)
        
        rdd = pysc.parallelize(data)
        item_obj = register_item(filename, rdd)    
        producer_obj.send("registered_user", item_obj)

        time.sleep(0.8)
    

    print("Kafka Producer Application Completed ... ")
