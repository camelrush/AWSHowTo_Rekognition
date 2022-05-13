import json
import boto3
import base64

rekognition = boto3.client('rekognition', 'ap-northeast-1')
iot = boto3.client('iot-data')

IOT_TOPIC = "topicFaceRekognition"

def lambda_handler(event, context):

    for record in event['Records']:

        tm_str = record['dynamodb']['NewImage']['GetDateTime']['S']
        img_bytes_base64 = record['dynamodb']['NewImage']['bytes']['S']
        img_bytes = base64.b64decode(img_bytes_base64.encode('utf-8'))

        try:
            response = rekognition.detect_faces(
                Image={
                    'Bytes':img_bytes
                },
                Attributes=[
                    "ALL",
                ]
            )
    
            for faceDetail in response["FaceDetails"]:

                print("Gender：%s(%.2f)" % (faceDetail["Gender"]["Value"],faceDetail["Gender"]["Confidence"]))
                print("Age   ：%d~%d" % (faceDetail["AgeRange"]["Low"],faceDetail["AgeRange"]["High"]))
                print("Smile ：%s(%.2f)" % (faceDetail["Smile"]["Value"],faceDetail["Smile"]["Confidence"]))
                
                payload = {
                    "GetDateTime": tm_str,
                    "Gender": faceDetail["Gender"]["Value"],
                    "Age": str(faceDetail["AgeRange"]["Low"]) + "~" + str(faceDetail["AgeRange"]["High"])
                }
                
                iot.publish(
                    topic=IOT_TOPIC,
                    qos=1,
                    payload=json.dumps(payload, ensure_ascii=False)
                )
                
                return True
                
        except Exception as e:
            print('exception:' + str(e))
            raise e
