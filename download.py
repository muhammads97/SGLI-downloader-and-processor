import requests
from tqdm import tqdm
import asyncio
import concurrent.futures
import functools
import os
import json

class Download:
  def __init__(self, token, account, password):
    self.token = token
    self.account = account
    self.password = password
    self.headers = {
      "Accept": "*/*",
      "Accept-Encoding": "*",
      "Connection": "keep-alive",
      "Cookie": "fuel_csrf_token=%s"%token
    }
    self.lock = asyncio.Lock()

  def auth(self):
    self.headers["Cookie"] = "fuel_csrf_token=%s"%self.token
    body = {
      "account": self.account,
      "password": self.password,
      "fuel_csrf_token": self.token
    } 
    auth_url = "https://gportal.jaxa.jp/gpr/auth/authenticate.json"
    res = requests.post(auth_url, body, headers = self.headers)
    if res.ok:
      cookie = res.headers["Set-Cookie"].split("secure, ")[-1]
      print("auth completed, cookie: " + cookie)
      self.headers["Cookie"] = cookie
    else:
      print("auth failed ! : ") 
      # print(res.content)
      # print(res.request.body)
      # print(res.request.method)
      # print(res.request.headers)
      # print(res.status_code)

  async def get_size(self, url):
    response = requests.head(url, headers=self.headers)
    size = int(response.headers['Content-Length'])
    return size

  def download_range(self, url, start, end, output):
    headers = {'Range': f'bytes={start}-{end}'}
    headers.update(self.headers)
    response = requests.get(url, headers=headers)

    with open(output, 'wb') as f:
        for part in response.iter_content(1024):
            f.write(part)
    self.pbar.update(1)

  async def download(self, run, loop, url, output, chunk_size=1000000):
    file_size = await self.get_size(url)
    chunks = range(0, file_size, chunk_size)
    self.pbar = tqdm(total=len(chunks))
    tasks = [
        run(
            self.download_range,
            url,
            start,
            start + chunk_size - 1,
            f'{output}.part{i}',
        )
        for i, start in enumerate(chunks)
    ]

    await asyncio.wait(tasks)
    self.pbar.close()
    with open(output, 'wb') as o:
        for i in range(len(chunks)):
            chunk_path = f'{output}.part{i}'

            with open(chunk_path, 'rb') as s:
                o.write(s.read())

            os.remove(chunk_path)


  def get(self, url, out, max_workers=3):
    self.auth()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    loop = asyncio.new_event_loop()
    run = functools.partial(loop.run_in_executor, executor)

    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(
            self.download(run, loop, url, out)
        )
    finally:
        loop.close()