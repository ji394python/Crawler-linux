import requests
#只有isu_cd不一樣
url = "https://global.krx.co.kr/contents/COM/GenerateOTP.jspx?name=fileDown&filetype=xls&url=GLB/05/0507/0507010104/glb0507010104_02&acsString=1&gubun=00&isu_cd=KR7152100004&pagePath=/contents/GLB/05/0507/0507010104/GLB0507010104.jsp"
params = {
  
}
payload={}
headers = {
  'Cookie': 'SCOUTER=x3dstqs596g8in; JSESSIONID=8B838AFB4FF773B9DDAE479CA6C3E83F.103tomcat2',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36 Edg/93.0.961.47',
    'x-requested-with': 'XMLHttpRequest',
    'referer': 'https://global.krx.co.kr/contents/GLB/05/0507/0507010104/GLB0507010104.jsp'
}

response = requests.request("GET", url, headers=headers, data=payload)
new =response.text


data = {'code':new}
header = {
    'origin': 'https://global.krx.co.kr',
    'referer': 'https://global.krx.co.kr/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36 Edg/93.0.961.47',
    'content-type': 'application/x-www-form-urlencoded',
    'cookie': 'SCOUTER=zt74gd60lpfrv; JSESSIONID=83BDD0F077993098F6DB99042CC0B1DA.57tomcat4'
}
r = requests.request('POST','https://file.krx.co.kr/download.jspx',data=data,headers=header)
with open('test.xls','wb+') as f:
    f.write(r.content)
    f.close()