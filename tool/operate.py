import os 

# os相關的操作放在這
# 1. 檢查目錄是否存在
def pathControl(path:str):
    
    def dirCreate(path:str):
        if os.path.exists(path)==False:
            os.mkdir(path) #建立base目錄\
    length = path.split('/') #parsing path

    if len(length) == 1: 
        if length[0].find('.') != -1:
            return('')
        dirCreate(length[0])
        return('')

    bool = 1 if length[-1].find('.') != -1 else 1
    length = length[:len(length)-bool]
    nums = length.count('..')
    if nums != 0:
        temp = '/'.join(length[:nums+1])
        length = length[nums+1:]
    else:
        temp = length[0]
        length = length[1:]
    dirCreate(temp)

    for i in length:
        temp = temp + '/' + i
        dirCreate(temp)