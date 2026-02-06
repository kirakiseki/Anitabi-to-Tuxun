import httpx
import asyncio
from dataclasses import dataclass
from loguru import logger
from typing import List, List, Optional
import dotenv
import os
import csv

dotenv.load_dotenv()

@dataclass
class Point:
    lat: float
    lng: float

@dataclass
class AnitabiPoint:
    name: str
    geo: Point
    

@dataclass
class GooglePanoPoint:
    geo: Point
    pano_id: str
    
    @property
    def url(self) -> str:
        return f"https://www.google.com/maps/@{self.geo.lat},{self.geo.lng},3a/data=!3m8!1e1!3m6!1s{self.pano_id}!2e10!3e12!6s"
    
    def __eq__(self, value): # For deduplication
        if not isinstance(value, GooglePanoPoint):
            return False
        
        return self.pano_id == value.pano_id
    
    def __hash__(self):
        return hash(self.pano_id)
    
@dataclass
class Record:
    anitabi_point: AnitabiPoint
    google_pano_point: GooglePanoPoint
    
    def __eq__(self, value):
        if not isinstance(value, Record):
            return False
        
        return self.google_pano_point == value.google_pano_point

    def __hash__(self):
        return hash(self.google_pano_point)

def fetch_and_parse_points(bangumi_id: int) -> List[AnitabiPoint]:
    """
    请求 API 并提取每个 point 的 name 和 geo。
    """
    url = f"https://api.anitabi.cn/bangumi/{bangumi_id}/points"
    results = []

    logger.info(f"正在请求接口: {url}")

    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.get(url)

            # 检查 HTTP 状态码
            response.raise_for_status()

            # 解析 JSON
            data = response.json()

            # 获取 points 列表
            points = data.get("points", [])
            logger.info(f"成功获取数据，共计 {len(points)} 个点位")

            # 遍历并提取关键字段
            for point in points:
                geo = point["geo"]
                lat, lng = geo[0], geo[1]  # geo: [lat, lng]
                    
                results.append(AnitabiPoint(
                    name=point.get("name"),
                    geo=Point(lat=lat, lng=lng)
                ))

            logger.success(f"解析完成，成功提取 {len(results)} 条记录")

    except httpx.HTTPStatusError as e:
        logger.error(
            f"HTTP 请求失败: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        logger.exception(f"处理过程中发生异常: {e}")

    return results


async def get_google_pano(client: httpx.AsyncClient, p: Point, api_key: str, max_radius: int = 50) -> Optional[GooglePanoPoint]:
    """
    通过经纬度获取最近的 Street View PanoID
    """
    lat, lng = p.lat, p.lng
    metadata_url = "https://maps.googleapis.com/maps/api/streetview/metadata"
    params = {
        "location": f"{lat},{lng}",
        "key": api_key,
        "radius": max_radius  # 搜索 max_radius 米范围内的街景
    }

    try:
        resp = await client.get(metadata_url, params=params)
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") == "OK":
            pano_id = data["pano_id"]
            location = data["location"]
            real_lat = location.get("lat", lat)
            real_lng = location.get("lng", lng)
            
            logger.debug(f"坐标 ({lat}, {lng}) 找到 PanoID: {pano_id}")
            return GooglePanoPoint(
                geo=Point(lat=real_lat, lng=real_lng),
                pano_id=pano_id
            )
        else:
            logger.warning(
                f"坐标 ({lat}, {lng}) 未找到街景: {data.get('status')}")
            return None
    except Exception as e:
        logger.error(f"查询 PanoID 失败: {e}")
        return None


async def process_points_with_pano(points_list: List[AnitabiPoint], api_key: str) -> List[Record]:
    final_data = []
    
    limits = httpx.Limits(max_connections=1, max_keepalive_connections=1) # 根据需要调整连接池大小
    async with httpx.AsyncClient(limits=limits, timeout=10.0) as client:
        tasks = []
        for pt in points_list:
            tasks.append(get_google_pano(client, pt.geo, api_key, max_radius=50))
            
        logger.info(f"开始并发请求 Google API, 总计 {len(tasks)} 个任务...")
        pano_results = await asyncio.gather(*tasks)
        logger.info("Google API 请求完成，开始处理结果...")
        
        for pt, pano_point in zip(points_list, pano_results):
            if pano_point:
                final_data.append(Record(anitabi_point=pt, google_pano_point=pano_point))
            else:
                logger.warning(f"未找到街景，跳过点位: {pt.name} ({pt.geo.lat}, {pt.geo.lng})")
                
    return final_data

async def main():
    # 吹 TV 1,2,3 + 剧场版 誓 / 合 / 传
    BANGUMI_IDS = [115908, 152091, 283643, 216372, 386195, 211089]
    DEDUPLICATE_PANO = True
    CSV_OUTPUT_FILE = "output.csv"
    TUXUN_OUTPUT_FILE = "tuxun_output.txt"
    
    # 可以从环境变量中获取 API Key 或直接在此处填写
    GOOGLE_MAPS_API_KEY = os.getenv(
        "GOOGLE_MAPS_API_KEY") or "YOUR_GOOGLE_MAPS_API_KEY_HERE"
    logger.debug(f"使用的 Google Maps API Key: {GOOGLE_MAPS_API_KEY}")

    points_list = []
    for bangumi_id in BANGUMI_IDS:
        logger.info(f"处理番剧 ID: {bangumi_id}")
        points = fetch_and_parse_points(bangumi_id)
        points_list.extend(points)
        
    records = await process_points_with_pano(points_list, GOOGLE_MAPS_API_KEY)
    
    # 去重
    if DEDUPLICATE_PANO:
        logger.info(f"去重前共有 {len(records)} 条记录")
        records = list(set(records))
        logger.info(f"去重后共有 {len(records)} 条记录")
    
    # 输出结果
    tuxun_urls = []
    with open(CSV_OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['name', 'anitabi_lat', 'anitabi_lng', 'pano_id', 'pano_lat', 'pano_lng', 'google_maps_url']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for record in records:
            writer.writerow({
                'name': record.anitabi_point.name,
                'anitabi_lat': record.anitabi_point.geo.lat,
                'anitabi_lng': record.anitabi_point.geo.lng,
                'pano_id': record.google_pano_point.pano_id,
                'pano_lat': record.google_pano_point.geo.lat,
                'pano_lng': record.google_pano_point.geo.lng,
                'google_maps_url': record.google_pano_point.url
            })
            tuxun_urls.append(record.google_pano_point.url)
    
    with open(TUXUN_OUTPUT_FILE, mode='w', encoding='utf-8') as tuxun_file:
        tuxun_file.write("\n".join(tuxun_urls))

if __name__ == "__main__":
    asyncio.run(main())