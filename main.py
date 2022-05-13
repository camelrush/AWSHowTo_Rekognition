import cv2
import json
import time
import datetime
import asyncio
import base64
import paho.mqtt.client
import ssl

# MQTT定義
AWSIoT_ENDPOINT = "alij9rhkrwgll-ats.iot.ap-northeast-1.amazonaws.com"
MQTT_PORT = 8883
MQTT_TOPIC_PUB = "topicFaceDetect"
MQTT_TOPIC_SUB = "topicFaceRekognition"
MQTT_ROOTCA = "../AWSIoTCoreCert/AmazonRootCA1.pem"
MQTT_CERT = "../AWSIoTCoreCert/d9b8932398a01d603702502e2293d52adb8a6b25a3ee4ee935eb511c704b662f-certificate.pem.crt"
MQTT_PRIKEY = "../AWSIoTCoreCert/d9b8932398a01d603702502e2293d52adb8a6b25a3ee4ee935eb511c704b662f-private.pem.key"

# カスケード分類器のパス
CASCADE_FACE_PATH="./cascade_data/haarcascade_frontalface_alt.xml"
CASCADE_EYE_PATH="./cascade_data/haarcascade_eye.xml"

# 顔検出タイムアウト(秒)    
REKOGNITION_TIMEOUT_SEC = 3

# カスケード分類器を取得
cascade_face=cv2.CascadeClassifier(CASCADE_FACE_PATH)
cascade_eye=cv2.CascadeClassifier(CASCADE_EYE_PATH)

# カメラからの画像データの読み込み
capture = cv2.VideoCapture(0,cv2.CAP_MSMF)
capture.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc('H', '2', '6', '4'))
capture.set(cv2.CAP_PROP_BUFFERSIZE , 1)
capture.set(cv2.CAP_PROP_FRAME_WIDTH ,1024)
capture.set(cv2.CAP_PROP_FRAME_HEIGHT,768)
capture.set(cv2.CAP_PROP_FPS ,60)

frame_width = capture.get(cv2.CAP_PROP_FRAME_WIDTH)
frame_height = capture.get(cv2.CAP_PROP_FRAME_HEIGHT)

# 顔認識情報の初期化
is_rekognition_received = False
rekognition_result_gender = ""
rekognition_result_agerange = ""

# MQTT接続イベント コールバック 
def mqtt_connect(client, userdata, flags, respons_code):
    print('mqtt connected.') 
    client.subscribe(MQTT_TOPIC_SUB)
    print('subscribe topic : ' + MQTT_TOPIC_SUB) 

# MQTTサブスクライブ コールバック
def mqtt_message(client, userdata, msg):
    global rekognition_result_gender
    global rekognition_result_agerange
    global is_rekognition_received

    # 受信データを取得
    json_dict = json.loads(msg.payload)
    rekognition_result_gender = str(json_dict["Gender"])
    rekognition_result_agerange = str(json_dict["Age"])
    print(format(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S") + " " + str(json_dict))

    # 受信フラグON
    is_rekognition_received = True

def mosaic(src, ratio=0.1):
    small = cv2.resize(src, None, fx=ratio, fy=ratio, interpolation=cv2.INTER_NEAREST)
    return cv2.resize(small, src.shape[:2][::-1], interpolation=cv2.INTER_NEAREST)

def mosaic_area(src, x, y, width, height, ratio=0.1):
    dst = src.copy()
    dst[y:y + height, x:x + width] = mosaic(dst[y:y + height, x:x + width], ratio)
    return dst

# 顔認識ループ
async def face_detect_loop():
    global rekognition_result_gender
    global rekognition_result_agerange
    global is_rekognition_received
    # リアルタイム静止画像の読み取りを繰り返す
    while(True):
        #フレームの読み取り
        ret,frame=capture.read()

        #カメラから読み取った画像をグレースケールに変換
        gray=cv2.cvtColor(frame,cv2.COLOR_BGR2GRAY)

        #顔の学習データ精査
        front_face_list=cascade_face.detectMultiScale(gray,minSize=(50,50))

        #顔と認識する場合は顔認識OKと出力
        if len(front_face_list) != 0:
            BORDER_COLOR = (255, 255, 255) # 線色を白に
            for rect in front_face_list:
                # 顔検出した部分に枠を描画
                cv2.rectangle(
                    frame,
                    tuple(rect[0:2]),
                    tuple(rect[0:2] + rect[2:4]),
                    BORDER_COLOR,
                    thickness=2
                )

                #目の学習データ精査
                eye_list=cascade_eye.detectMultiScale(gray,minSize=(50,50))
                for eye_rect in eye_list:
                    frame = mosaic_area(frame ,eye_rect[0],eye_rect[1],eye_rect[2],eye_rect[3],0.1)

        cv2.imshow("capture",frame)

        kin = cv2.waitKey(1)

        #escキーで終了
        if kin == 27:
            quit()

        #顔検出状態でspaceキー押下→保存
        if (len(front_face_list) != 0) and (kin == 32):

            # jpg形式にエンコード
            is_success , frame_jpg_bin = cv2.imencode(".jpg" ,frame)

            # MQTTでAWSへ送信
            if is_success == True:
                tm_str = format(datetime.datetime.now(),"%Y-%m-%d %H:%M:%S")
                json_msg = json.dumps({"GetDateTime": tm_str,
                                       "bytes": base64.b64encode(frame_jpg_bin).decode('utf-8')})
                client.publish(MQTT_TOPIC_PUB ,json_msg)
            else: 
                # エンコードエラー
                print("cv2.imencode() is failed!")

            # 識別完了待ち
            is_rekognition_received = False
            wait_sec = 1
            while(is_rekognition_received == False):
                # 5秒応答がなければタイムアウト
                wait_sec = wait_sec + 1
                if wait_sec > REKOGNITION_TIMEOUT_SEC:
                    break
                time.sleep(1)

            # 結果表示
            if wait_sec <= REKOGNITION_TIMEOUT_SEC:
                info = "Sex:{0:} / Age:{1:}".format(rekognition_result_gender,rekognition_result_agerange)

                cv2.putText(img=frame,
                    text=info,
                    org=(150,440),
                    fontFace=cv2.FONT_HERSHEY_SIMPLEX,
                    fontScale=1.0,
                    color=(64,64,255),
                    thickness=2,
                    lineType=cv2.LINE_4)
                cv2.imshow("capture",frame)
            else:
                cv2.imshow("capture",gray)

            # 結果表示完了待ち
            cv2.waitKey(1)
            time.sleep(3)

        time.sleep(0.01)

# Main Procedure
if __name__ == '__main__':
    # Mqtt Client Initialize
    client = paho.mqtt.client.Client()
    client.on_connect = mqtt_connect
    client.on_message = mqtt_message
    client.tls_set(MQTT_ROOTCA, certfile=MQTT_CERT, keyfile=MQTT_PRIKEY, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)

    # Connect To Mqtt Broker(aws)
    client.connect(AWSIoT_ENDPOINT, port=MQTT_PORT, keepalive=60)

    # Start Mqtt Subscribe 
    client.loop_start()

    # Start Publish Loop 
    loop = asyncio.get_event_loop()
    loop.run_until_complete(face_detect_loop())
