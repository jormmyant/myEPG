import xml.etree.ElementTree as ET
from collections import defaultdict
import aiohttp
import asyncio
from tqdm.asyncio import tqdm_asyncio
from datetime import datetime
import gzip
import shutil
from xml.dom import minidom
import re
from opencc import OpenCC
import os
from tqdm import tqdm
import hashlib

# 预初始化 OpenCC 转换器，避免重复创建对象带来的开销
cc = OpenCC('t2s')

def transform2_zh_hans(string):
    """安全的简繁体转换函数，增加空值检查"""
    if string is None:
        return None
    if not isinstance(string, str):
        print(f"Warning: Expected string for conversion, got {type(string)}. Returning original value.")
        return string
    try:
        return cc.convert(string)
    except Exception as e:
        print(f"Convert to zh_hans failed: {e}. Input was: '{string}'")
        return string

def get_content_hash(epg_content):
    """生成EPG内容的哈希值，用于缓存标识"""
    return hashlib.md5(epg_content.encode('utf-8')).hexdigest()

# 简单的内存缓存字典
epg_cache = {}

async def fetch_epg(url):
    """获取EPG数据的异步函数"""
    connector = aiohttp.TCPConnector(limit=16, ssl=False)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
    }
    try:
        async with aiohttp.ClientSession(connector=connector, trust_env=True, headers=headers) as session:
            async with session.get(url, timeout=30) as response:
                response.raise_for_status()
                return await response.text(encoding='utf-8')
    except aiohttp.ClientError as e:
        print(f"{url} HTTP请求错误: {e}")
    except asyncio.TimeoutError:
        print(f"{url} 请求超时")
    except Exception as e:
        print(f"{url} 其他错误: {e}")
    return None

def parse_epg(epg_content, use_cache=True):
    """
    解析EPG内容，支持缓存
    """
    if epg_content is None or epg_content.strip() == "":
        print("EPG内容为空，跳过解析")
        return {}, defaultdict(list)

    # 缓存检查
    content_hash = get_content_hash(epg_content)
    if use_cache and content_hash in epg_cache:
        print("命中缓存，直接返回缓存结果")
        return epg_cache[content_hash]

    channels = {}
    programmes = defaultdict(list)

    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        print(f"Problematic content: {epg_content[:500]}")  
        return {}, defaultdict(list)

    # 处理频道信息
    for channel in root.findall('channel'):
        channel_id = transform2_zh_hans(channel.get('id'))
        if channel_id:
            display_name_elem = channel.find('display-name')
            display_name = transform2_zh_hans(display_name_elem.text if display_name_elem is not None else '')
            channels[channel_id] = display_name

    # 处理节目信息
    for programme in root.findall('programme'):
        channel_id = transform2_zh_hans(programme.get('channel'))
        if not channel_id:
            continue
            
        start_time = programme.get('start')
        stop_time = programme.get('stop')
        if not start_time or not stop_time:
            continue
            
        try:
            # 清理时间字符串中的空格
            start_time_clean = re.sub(r'\s+', '', start_time)
            stop_time_clean = re.sub(r'\s+', '', stop_time)
            
            channel_start = datetime.strptime(start_time_clean, "%Y%m%d%H%M%S%z")
            channel_stop = datetime.strptime(stop_time_clean, "%Y%m%d%H%M%S%z")
        except ValueError as e:
            print(f"时间格式错误: {e}, 跳过该节目")
            continue

        title_elem = programme.find('title')
        if title_elem is None or title_elem.text is None:
            continue
            
        channel_title = transform2_zh_hans(title_elem.text)

        # 创建新的节目元素
        programme_elem = ET.Element('programme')
        programme_elem.set("channel", channel_id)
        programme_elem.set("start", channel_start.strftime("%Y%m%d%H%M%S +0800"))
        programme_elem.set("stop", channel_stop.strftime("%Y%m%d%H%M%S +0800"))
        
        title_elem_new = ET.SubElement(programme_elem, 'title')
        title_elem_new.text = channel_title

        # 处理描述信息
        desc_elem = programme.find('desc')
        if desc_elem is not None and desc_elem.text is not None:
            channel_desc = transform2_zh_hans(desc_elem.text)
            desc_elem_new = ET.SubElement(programme_elem, 'desc')
            desc_elem_new.text = channel_desc

        programmes[channel_id].append(programme_elem)

    # 缓存结果
    if use_cache:
        epg_cache[content_hash] = (channels, programmes)
        print("解析结果已缓存")

    return channels, programmes

def write_to_xml(channels, programmes, filename):
    """写入XML文件"""
    if not os.path.exists('output'):
        os.makedirs('output')
        
    current_time = datetime.now().strftime("%Y%m%d%H%M%S +0800")
    root = ET.Element('tv', attrib={'date': current_time})
    
    # 添加频道信息
    for channel_id, display_name in channels.items():
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": channel_id})
        display_name_elem = ET.SubElement(channel_elem, 'display-name', attrib={"lang": "zh"})
        display_name_elem.text = display_name
    
    # 添加节目信息
    for channel_id, prog_list in programmes.items():
        for prog in prog_list:
            prog.set('channel', channel_id)
            root.append(prog)

    # Beautify the XML output
    rough_string = ET.tostring(root, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(reparsed.toprettyxml(indent='\t', newl='\n'))

def compress_to_gz(input_filename, output_filename):
    """压缩文件为gz格式"""
    try:
        with open(input_filename, 'rb') as f_in:
            with gzip.open(output_filename, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"文件已压缩: {output_filename}")
    except Exception as e:
        print(f"压缩文件时出错: {e}")

def get_urls():
    """获取URL列表，增加文件存在检查"""
    if not os.path.exists('config.txt'):
        print("配置文件 config.txt 不存在")
        return []
        
    urls = []
    try:
        with open('config.txt', 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if line and not line.startswith('#'):
                    urls.append(line)
    except Exception as e:
        print(f"读取配置文件时出错: {e}")
        
    return urls

async def main():
    try:
        urls = get_urls()
        if not urls:
            print("没有找到有效的URL，程序退出")
            return
            
        print(f"找到 {len(urls)} 个EPG数据源")
        
        # 创建任务列表
        tasks = [fetch_epg(url) for url in urls]
        print("Fetching EPG data...")
        
        # 使用tqdm_asyncio.gather并发执行任务
        epg_contents = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")
        
        all_channels = {}
        all_programmes = defaultdict(list)
        
        print("Parsing EPG data...")
        successful_parses = 0
        
        # 使用tqdm显示解析进度
        for i, epg_content in enumerate(tqdm(epg_contents, desc="Parsing EPG")):
            if epg_content is None:
                print(f"URL {i+1} 返回空内容")
                continue
                
            try:
                channels, programmes = parse_epg(epg_content)
                if channels:
                    all_channels.update(channels)
                    for channel_id, prog_list in programmes.items():
                        all_programmes[channel_id].extend(prog_list)
                    successful_parses += 1
            except Exception as e:
                print(f"解析 URL {i+1} 时出错: {e}")
        
        print(f"成功解析 {successful_parses}/{len(urls)} 个EPG源")
        
        if not all_channels:
            print("没有成功解析到任何频道信息，程序退出")
            return
            
        print("Writing to XML...")
        write_to_xml(all_channels, all_programmes, 'output/epg.xml')
        
        print("Compressing to gz...")
        compress_to_gz('output/epg.xml', 'output/epg.gz')
        
        print("EPG生成完成！")
        
    except Exception as e:
        print(f"程序执行出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(main())
