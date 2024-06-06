import os
import json
from datetime import timedelta
from google.cloud import storage
import re

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "temp/gcs-user-key.json"


class GCSUtils:
    def __init__(self, bucket_name):
        """初始化傳入存儲桶名稱"""
        self._bucket_name = bucket_name

    # 暫時沒用到，只是測試用，可考慮要不要實作，如果要用可以用 search_objects 來達到相同目的
    def list_objects(self):
        """列出指定存儲桶中的所有對象"""
        client = storage.Client()
        bucket = client.bucket(self._bucket_name)
        blobs = bucket.list_blobs()
        return [blob.name for blob in blobs]

    def upload_files(self, files_info):
        """上傳單個或多個文件到指定目錄中
        - files_info: 一個或多個文件的資訊，需包含 source_path 和 destination_path
        """
        client = storage.Client()
        bucket = client.bucket(self._bucket_name)
        results = []

        for file_info in files_info:
            blob = bucket.blob(file_info["destination_path"])
            blob.upload_from_filename(file_info["source_path"])
            results.append(
                f"File {file_info['source_path']} uploaded to {file_info['destination_path']}."
            )

        return results

    def delete_files(self, object_names):
        """刪除存儲桶中的一個或多個文件，object_names 是文件名的列表"""
        client = storage.Client()
        bucket = client.bucket(self._bucket_name)
        results = []

        # 刪除每個指定的文件
        for object_name in object_names:
            blob = bucket.blob(object_name)
            blob.delete()
            results.append(f"已刪除文件：{object_name}")

        return results

    def delete_folders(self, folders):
        """刪除存儲桶中的一個或多個目錄及其內的所有文件"""
        client = storage.Client()
        bucket = client.bucket(self._bucket_name)
        results = []

        for dir_path in folders:
            # 確保目錄路徑以 '/' 結尾
            if not dir_path.endswith("/"):
                dir_path += "/"

            blobs = bucket.list_blobs(prefix=dir_path)
            blobs_to_delete = [blob for blob in blobs]  # 收集所有要刪除的 blob 對象

            if blobs_to_delete:
                bucket.delete_blobs(blobs_to_delete)  # 批量刪除操作
                results.append(f"已刪除目錄 '{dir_path}' 及其內所有文件。")
            else:
                results.append(f"目錄 '{dir_path}' 為空或不存在。")

        return results

    def search_objects(self, pattern, folder_name=""):
        """
        搜尋指定目錄的對象，可指定搜尋目錄，但不可搜尋目錄
        使用 list_objects_with_metadata 傳入 recursive=True 來達到目的
        - pattern: 搜尋的字串，會跳脫特殊字元防止正則表達式錯誤
        - folder_name: 指定目錄，預設為空字串
        """
        # 將 pattern 轉換成正則表達式 防止特殊字元
        pattern = re.escape(pattern)
        regex = re.compile(f".*/([^/]*{pattern}[^/]*)$")

        # 若搜尋結果為空，則回傳空列表
        return [
            blob
            for blob in self.list_objects_with_metadata(
                folder_name=folder_name, recursive=True
            ).get("objects_info")
            or []
            if regex.search(blob.get("path"))
        ]

    def get_object_info(self, object_name):
        """
        獲取存儲桶中指定對象的資訊，可判斷是文件還是目錄
        - is_directory: True 表示是目錄，False 表示是文件
        """
        client = storage.Client()
        bucket = client.bucket(self._bucket_name)
        blob = bucket.get_blob(object_name)
        if blob is None:
            # 看要回啥給前端
            return "No object found."

        return {
            "name": blob.name.rsplit("/", 1)[1] if blob.name else "",
            "path": blob.name,
            "size": blob.size,
            "content_type": blob.content_type,
            "time_created": (
                blob.time_created.strftime("%Y-%m-%d %H:%M:%S")
                if blob.time_created
                else ""
            ),
            "time_updated": (
                blob.updated.strftime("%Y-%m-%d %H:%M:%S") if blob.updated else ""
            ),
            "signed_url": self._generate_signed_url(blob.name),
            "is_directory": blob.name.endswith("/") if blob.name else False,
        }

    def list_objects_with_metadata(
        self,
        folder_name="",
        page_size=None,
        page_token=None,
        recursive=False,
    ):
        """
        功能說明：列出指定存儲桶中所有對象的名稱及其 metadata 可區分文件和目錄
        - 傳入的值 folder_name 必須有 "/" 作為後綴
        - is_directory: True 表示是目錄，False 表示是文件
        - singned_url 必須透過 get_object_info 來取得，節省流量
        - ref: https://cloud.google.com/storage/docs/samples/storage-list-files-with-prefix?hl=zh-cn

        參數說明
        - folder_name: 指定目錄，預設為空字串為根目錄
        - page_size: 每頁的對象數量
        - page_token: 要查看的當前頁的 token
        - recursive: 是否遞迴查詢，預設為 False，目前僅在 search_objects 中使用遞迴搜尋

        回傳
        - current_page_token: 當前頁的 token，若為 null 表示是第一頁
        - next_page_token: 下一頁的 token，若為 null 表示是最後一頁
        - objects_info: 對象的資訊列表，包含 name, path, size, content_type, time_created, time_updated, is_directory
        """
        client = storage.Client()
        bucket = client.bucket(self._bucket_name)
        delimiter = "/" if not recursive else None
        blobs = bucket.list_blobs(
            prefix=folder_name,
            max_results=page_size,
            page_token=page_token,
            delimiter=delimiter,
        )
        objects_info = []
        page = next(blobs.pages)
        next_page_token = blobs.next_page_token

        # 如果 blobs 有 prefixes 屬性，表示是目錄，目錄在前面，所以先做
        if hasattr(blobs, "prefixes"):
            for prefix in blobs.prefixes:
                objects_info.append(
                    {
                        "name": prefix.replace(folder_name, ""),
                        "full_name": prefix,
                        "is_directory": True,
                    }
                )

        for blob in page:
            if blob.name == folder_name:
                continue
            object_metadata = {
                "name": blob.name.rsplit("/", 1)[1],
                "path": blob.name,
                "size": blob.size,
                "content_type": blob.content_type,
                "time_created": blob.time_created.strftime("%Y-%m-%d %H:%M:%S"),
                "time_updated": blob.updated.strftime("%Y-%m-%d %H:%M:%S"),
                "is_directory": blob.name.endswith("/"),
            }
            objects_info.append(object_metadata)

        return {
            "current_page_token": page_token,
            "next_page_token": next_page_token,
            "objects_info": objects_info,
        }

    def move_file(self, source_blob_name, destination_blob_name):
        """移動檔案到儲存桶中的新位置"""
        client = storage.Client()
        bucket = client.bucket(self._bucket_name)

        # 取得源文件的 blob
        source_blob = bucket.blob(source_blob_name)

        # 複製源文件到新位置
        bucket.copy_blob(source_blob, bucket, destination_blob_name)

        # 刪除原文件
        source_blob.delete()

        return f"文件從 {source_blob_name} 移動到 {destination_blob_name} 成功。"

    def download_blob(self, source_blob_name, destination_file_name):
        """下載一個 blob 到本地檔案
        source_blob_name: 儲存桶檔案的路徑和名稱
        destination_file_name: 本機檔案的路徑和名稱
        """
        # 初始化 GCS 客戶端
        storage_client = storage.Client()

        # 獲取存儲桶
        bucket = storage_client.bucket(self._bucket_name)

        # 獲取 blob
        blob = bucket.blob(source_blob_name)

        # 下載 blob 到本地檔案
        blob.download_to_filename(destination_file_name)

        print(f"檔案 {source_blob_name} 已下載到 {destination_file_name}。")

    @staticmethod
    def move_file_across_buckets(
        source_bucket_name,
        destination_bucket_name,
        source_blob_name,
        destination_blob_name,
    ):
        """從一個儲存桶移動到另一個儲存桶"""
        client = storage.Client()

        # 獲取源儲存桶和目標儲存桶
        source_bucket = client.bucket(source_bucket_name)
        destination_bucket = client.bucket(destination_bucket_name)

        # 創建源文件的 blob
        source_blob = source_bucket.blob(source_blob_name)

        # 複製源文件到目標儲存桶
        destination_blob = source_bucket.copy_blob(
            source_blob, destination_bucket, destination_blob_name
        )

        # 刪除源文件
        source_blob.delete()

        return f"文件從 {source_bucket_name}/{source_blob_name} 移動到 {destination_bucket_name}/{destination_blob_name} 成功。"

    def _generate_signed_url(self, blob_name, expiration=3600):
        """
        生成簽名 URL，預設有效時間為 1 小時，目的是讓前端可以直接透過 URL 下載檔案
        因為 GCS 預設的對象是私有的，所以需要透過簽名 URL 來取得對象
        """
        client = storage.Client()
        bucket = client.bucket(self._bucket_name)
        blob = bucket.blob(blob_name)
        signed_url = blob.generate_signed_url(expiration=timedelta(seconds=expiration))
        return signed_url


# 範例使用方法
if __name__ == "__main__":
    # 初始化 GCSUtils 類 並指定存儲桶名稱
    gcs = GCSUtils("ops-payment-invoice")

    # # 取得特定對象的資訊
    # print(json.dumps(gcs.get_object_info("sonny_test/sonny_test_2/202405-2")))
    # # 遞迴搜尋特定對象，可指定目錄，搜尋結果僅限檔案不含目錄
    # print(json.dumps(gcs.search_objects("sonny", "")))
    # 列出指定目錄當前的所有對象
    # print(json.dumps(gcs.list_objects_with_metadata("sonny_test/", 10)))

    # print(gcs.list_objects())

    # # 刪除指定對象，刪了就沒了，請小心使用
    # gcs.delete_object("sonny_test/sonny_test_2/202405-2")

    # # 上傳檔案使用範例
    # # 單個檔案與多個檔案相同，都是傳入一個 list，裡面包含單個或多個 dict

    # # 上傳單個文件
    # single_file = [{'source_path': 'local/path/to/your/file.jpg', 'destination_path': 'path/in/bucket/file.jpg'}]
    # upload_results_single = gcs.upload_files(single_file)
    # print(upload_results_single)
    # # 上傳多個文件
    # multiple_files = [
    #     {'source_path': 'local/path/to/your/file1.jpg', 'destination_path': 'path/in/bucket/file1.jpg'},
    #     {'source_path': 'local/path/to/your/file2.jpg', 'destination_path': 'path/in/bucket/file2.jpg'}
    # ]
    # upload_results_multiple = gcs.upload_files(multiple_files)
    # print(upload_results_multiple)

    # # 刪除文件使用範例，刪了就沒了，請小心使用
    # # 刪除單個文件與多個文件相同，都是傳入一個 list，裡面包含單個或多個文件路徑
    # # 刪除單個文件
    # single_file_to_delete = ['path/to/your/file.jpg']
    # delete_results_single = gcs.delete_files(single_file_to_delete)
    # print(delete_results_single)
    # # 刪除多個文件
    # multiple_files_to_delete = ['path/to/your/file1.jpg', 'path/to/your/file2.jpg']
    # delete_results_multiple = gcs.delete_files(multiple_files_to_delete)
    # print(delete_results_multiple)

    # # 測試上傳 1-100 的檔案到 GCS
    # # 在 test 資料夾中產生 1-100 的檔案 內容為 1-100
    # for i in range(1, 101):
    #     with open(f"test/{i}.txt", "w") as f:
    #         f.write(str(i))

    # # 上傳 test 資料夾中的所有檔案到 GCS
    # files = [
    #     {"source_path": f"test/{file}", "destination_path": f"test/{file}"}
    #     for file in os.listdir("test")
    # ]
    # upload_results = gcs.upload_files(files)
    # print(upload_results)

    # # # 刪除 GCS 中 test 資料夾中的所有檔案
    # files_to_delete = [f"test/{file}" for file in os.listdir("test")]
    # delete_results = gcs.delete_files(files_to_delete)
    # print(delete_results)

    # # 刪除 GCS 中 test 資料夾 可單個或多個
    # folders_to_delete = ["test/"]
    # delete_results = gcs.delete_folders(folders_to_delete)
    # print(delete_results)

    # # 下載 GCS 中的檔案
    # 取得專案根目錄
    # project_root = os.path.dirname(os.path.abspath(__file__))
    # gcs.download_blob("test/1.txt", f"{project_root}/temp/1.txt")

    # 列出指定目錄當前的所有對象
    # print(json.dumps(gcs.list_objects_with_metadata("test/", 10)))
