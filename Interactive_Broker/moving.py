import os 

if __name__ == '__main__':
    base = 'Shortable/IB'
    for country in os.listdir(base):
        countryPath = base + '/' + country
        for file in os.listdir(countryPath):
            if file.find('.csv') != -1: 
                filePath = countryPath + '/' + file
                os.system('[ -d /path/to/dir ] && echo "Directory exists" || echo "Directory not exists"')
                os.path.exists()
