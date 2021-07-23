## 整理可轉債公告

library(dplyr)
library(lubridate)

dir_list <- list.files("../data/bond_news")
file_list <- c()
for(i in 1:length(dir_list)){
  a <- paste0("../data/bond_news/", dir_list[i])
  file_list_temp <- paste0(a, '/', list.files(a))
  file_list <- c(file_list, file_list_temp)
}
DT <- data.frame(file_path = file_list, stringsAsFactors=FALSE)

DT <- DT %>%
  mutate(date = ymd(substr(file_path, 19, 26)),
         name = NA,
         id_hat = NA)

for(i in 1:nrow(DT)){
  test <- readLines(DT$file_path[i])
  test_1 <- test[grepl('簡稱', test)]
  position_1 <- regexpr('簡', test_1)[1]
  position_2 <- regexpr('代', test_1)[1]
  DT$name[i] <- substr(test_1, (position_1 + 3), (position_2 - 2))
  DT$id_hat[i] <-  substr(test_1, (position_2 + 3), (position_2 + 7))
}

i <- 853
test <- readLines(DT$file_path[i])
test_1 <- test[grepl('簡稱', test)]
DT$name[i] <- '瑞基一'
DT$id_hat[i] <- '41711'

i <- 2440
test <- readLines(DT$file_path[i])
test_1 <- test[grepl('簡稱', test)]
DT$name[i] <- '動力二KY'
DT$id_hat[i] <- '65912'




DT_1 <- DT
DT_1$reason <- NA
for(i in 1:nrow(DT_1)){
  test <- readLines(DT_1$file_path[i])
  test_f <- c()
  test_temp <- c()
  
  for(j in 1:length(test)){
    if(substr(test[j], 1, 2) =='一、'){
      test_temp <- test[j]
    }else if(! substr(test[j], 1, 2) %in% c('一、','二、','三、',
                                            '四、','五、','六、',
                                            '七、','八、','九、')){
      test_temp <- paste0(test_temp, test[j])
    }else if(substr(test[j], 1, 2) %in% c('二、','三、',
                                          '四、','五、','六、',
                                          '七、','八、','九、')){
      test_f <- c(test_f, test_temp)
      test_temp <- test[j]
    }
    if(j == length(test)){
      test_f <- c(test_f, test_temp)
    }
  }
  test <- test_f
  
  if(length((test[grepl('十四條',test)])) > 0){
    DT_1$reason[i] <- T
  }else{
    DT_1$reason[i] <- F
  }
}
closeAllConnections()
DT_1 <- DT_1 %>%
  filter(reason == T)

DT_1$調整生效日 <- NA
DT_1$原訂轉換價 <- NA
DT_1$調整後轉換價 <- NA
DT_1$調整事由 <- NA
for(i in 1:nrow(DT_1)){
  test <- readLines(DT_1$file_path[i])
  test_1 <- test[grepl('主旨',test)]
  position_1 <- regexpr('自', test_1)[1]
  position_2 <- regexpr('價格自', test_1)[1]
  position_3 <- regexpr('調整為', test_1)[1]
  position_4 <- regexpr('元。', test_1)[1]
  DT_1$調整生效日[i] <- substr(test_1, (position_1 + 1), (position_1 + 9))
  DT_1$原訂轉換價[i] <- substr(test_1, (position_2 + 3), (position_3 -2))
  DT_1$調整後轉換價[i] <- substr(test_1, (position_3 + 3), (position_4 -1))
  
  test_2 <- test[grepl('公告事項',test)]
  if(grepl('現金股利|股息', test_2) & grepl('配發普通股', test_2)){
    DT_1$調整事由[i] <- '除權、除息'
  }else if(grepl('現金股利|股息', test_2)){
    DT_1$調整事由[i] <- '除息'
  }else if(grepl('現金增資', test_2)){
    DT_1$調整事由[i] <- '現金增資'
  }else if(grepl('配發普通股', test_2)){
    DT_1$調整事由[i] <- '除權'
  }
}
closeAllConnections()
DT_1$調整生效日 <- paste0(as.numeric(substr(DT_1$調整生效日, 1, 3)) + 1911,
                     '-',
                     substr(DT_1$調整生效日, 5, 6),
                     '-',
                     substr(DT_1$調整生效日, 8, 9))
DT_1 <- DT_1 %>%
  select(-file_path, -reason)
names(DT_1)[1] <- '公告日'
names(DT_1)[2] <- '可轉債名稱'
names(DT_1)[3] <- '可轉債代號'

write.csv(DT_1, '../data/CB_report/可轉債轉換價調整_20210512.csv')

# i <- 41
# test <- readLines(DT_1$file_path[i])

DT_2 <- DT
DT_2$reason <- NA
for(i in 1:nrow(DT_2)){
  test <- readLines(DT_2$file_path[i])
  test_f <- c()
  test_temp <- c()
  
  for(j in 1:length(test)){
    if(substr(test[j], 1, 2) =='一、'){
      test_temp <- test[j]
    }else if(! substr(test[j], 1, 2) %in% c('一、','二、','三、',
                                            '四、','五、','六、',
                                            '七、','八、','九、')){
      test_temp <- paste0(test_temp, test[j])
    }else if(substr(test[j], 1, 2) %in% c('二、','三、',
                                          '四、','五、','六、',
                                          '七、','八、','九、')){
      test_f <- c(test_f, test_temp)
      test_temp <- test[j]
    }
    if(j == length(test)){
      test_f <- c(test_f, test_temp)
    }
  }
  test <- test_f
  
  test_1 <- test[grepl('依據',test)]
  if(length((test_1[grepl('十八條',test_1)])) > 0){
    DT_2$reason[i] <- T
  }else{
    DT_2$reason[i] <- F
  }
}
closeAllConnections()
DT_2 <- DT_2 %>%
  filter(reason == T)

DT_2$強制贖回日 <- NA
DT_2$最後轉換日 <- NA
DT_2$贖回事由 <- NA
for(i in 1:nrow(DT_2)){
  test <- readLines(DT_2$file_path[i])
  test_f <- c()
  test_temp <- c()
  
  for(j in 1:length(test)){
    if(substr(test[j], 1, 2) =='一、'){
      test_temp <- test[j]
    }else if(! substr(test[j], 1, 2) %in% c('一、','二、','三、',
                                            '四、','五、','六、',
                                            '七、','八、','九、')){
      test_temp <- paste0(test_temp, test[j])
    }else if(substr(test[j], 1, 2) %in% c('二、','三、',
                                          '四、','五、','六、',
                                          '七、','八、','九、')){
      test_f <- c(test_f, test_temp)
      test_temp <- test[j]
    }
    if(j == length(test)){
      test_f <- c(test_f, test_temp)
    }
  }
  test <- test_f
  
  test_1 <- test[grepl('、公告事項',test)]
  test_2 <- test[grepl('、依據',test)]
  position_1 <- regexpr('行使債券贖回權|行使賣回權', test_1)[1]
  DT_2$強制贖回日[i] <- substr(test_1, (position_1 -10), (position_1 -2))
  
  if(grepl('第一項|30%|百分之三十', test_1) | grepl('第一項|30%|百分之三十', test_2)){
    DT_2$贖回事由[i] <- '連續三十天股價超過當時轉換價格30%以上'
  }else if(grepl('第二項|10%|百分之十', test_1) | grepl('第二項|10%|百分之十', test_2)){
    DT_2$贖回事由[i] <- '在外餘額低於原發行額度10%'
  }else if(grepl('發行屆滿', test[grepl('、主旨',test)])){
    DT_2$贖回事由[i] <- '發行屆滿'
  }
}
closeAllConnections()
DT_2$強制贖回日 <- paste0(as.numeric(substr(DT_2$強制贖回日, 1, 3)) + 1911,
                     '-',
                     substr(DT_2$強制贖回日, 5, 6),
                     '-',
                     substr(DT_2$強制贖回日, 8, 9))
DT_2 <- DT_2 %>%
  slice(-12)

DT_2 <- DT_2 %>%
  select(-file_path, -reason)
names(DT_2)[1] <- '公告日'
names(DT_2)[2] <- '可轉債名稱'
names(DT_2)[3] <- '可轉債代號'

write.csv(DT_2, '../data/CB_report/可轉債贖回_20210512.csv')


DT_3 <- DT
DT_3$reason <- NA
for(i in 1:nrow(DT_3)){
  # i <- 100
  test <- readLines(DT_3$file_path[i])
  test_f <- c()
  test_temp <- c()
  
  for(j in 1:length(test)){
    if(substr(test[j], 1, 2) =='一、'){
      test_temp <- test[j]
    }else if(! substr(test[j], 1, 2) %in% c('一、','二、','三、',
                                            '四、','五、','六、',
                                            '七、','八、','九、')){
      test_temp <- paste0(test_temp, test[j])
    }else if(substr(test[j], 1, 2) %in% c('二、','三、',
                                          '四、','五、','六、',
                                          '七、','八、','九、')){
      test_f <- c(test_f, test_temp)
      test_temp <- test[j]
    }
    if(j == length(test)){
      test_f <- c(test_f, test_temp)
    }
  }
  test <- test_f
  
  test_1 <- test[grepl('、公告',test)]
  test_2 <- test[grepl('、主旨',test)]
  if(length((test_1[grepl('停止受理轉換',test_1) | grepl('停止受理轉換',test_2)])) > 0){
    DT_3$reason[i] <- T
  }else{
    DT_3$reason[i] <- F
  }
}
closeAllConnections()
DT_3 <- DT_3 %>%
  filter(reason == T)

DT_3$停止轉換起日 <- NA
DT_3$停止轉換迄日 <- NA
DT_3$停止轉換事由 <- NA
for(i in 1:nrow(DT_3)){
  test <- readLines(DT_3$file_path[i])
  test_f <- c()
  test_temp <- c()
  position_1 <- 0
  position_2 <- 0
  position_3 <- 0
  
  for(j in 1:length(test)){
    if(substr(test[j], 1, 2) =='一、'){
      test_temp <- test[j]
    }else if(! substr(test[j], 1, 2) %in% c('一、','二、','三、',
                                            '四、','五、','六、',
                                            '七、','八、','九、')){
      test_temp <- paste0(test_temp, test[j])
    }else if(substr(test[j], 1, 2) %in% c('二、','三、',
                                          '四、','五、','六、',
                                          '七、','八、','九、')){
      test_f <- c(test_f, test_temp)
      test_temp <- test[j]
    }
    if(j == length(test)){
      test_f <- c(test_f, test_temp)
    }
  }
  test <- test_f
  
  test_1 <- test[grepl('、公告事項',test)]
  test_2 <- test[grepl('、依據',test)]
  position_1 <- regexpr('起訖日期', test_1)[1]
  position_2 <- regexpr('日至', test_1)[1]
  position_3 <- regexpr('日止|日停止轉換', test_1)[1]
  DT_3$停止轉換起日[i] <- substr(test_1, (position_1 + 5), (position_1 + 13))
  DT_3$停止轉換迄日[i] <- substr(test_1, (position_3 - 9), (position_3 - 1))
  
  if(grepl('股東臨時會', test_1)){
    DT_3$停止轉換事由[i] <- '股東臨時會'
  }else if(grepl('股東常會', test_1)){
    DT_3$停止轉換事由[i] <- '股東常會'
  }else if(grepl('現金增資', test_1)){
    DT_3$停止轉換事由[i] <- '現金增資'
  }else if(grepl('配股|配息|除息', test_1)){
    DT_3$停止轉換事由[i] <- '配股配息'
  }else if(grepl('辦理其他作業事宜', test_1)){
    DT_3$停止轉換事由[i] <- '辦理其他作業事宜'
  }
}
closeAllConnections()

i <- 464
DT_3$停止轉換起日[i] <- '108年04月11'
DT_3$停止轉換迄日[i] <- '108年05月06'

i <- 471
DT_3$停止轉換起日[i] <- '108年04月07'
DT_3$停止轉換迄日[i] <- '108年05月17'

i <- 282
DT_3$停止轉換起日[i] <- '107年08月06'
DT_3$停止轉換迄日[i] <- '107年08月15'

i <- 434
DT_3$停止轉換起日[i] <- '108年04月15'
DT_3$停止轉換迄日[i] <- '108年05月09'

i <- 771
DT_3$停止轉換起日[i] <- '109年04月07'



DT_3$停止轉換起日 <- paste0(as.numeric(substr(DT_3$停止轉換起日, 1, 3)) + 1911,
                      '-',
                      substr(DT_3$停止轉換起日, 5, 6),
                      '-',
                      substr(DT_3$停止轉換起日, 8, 9))
DT_3$停止轉換迄日 <- paste0(as.numeric(substr(DT_3$停止轉換迄日, 1, 3)) + 1911,
                      '-',
                      substr(DT_3$停止轉換迄日, 5, 6),
                      '-',
                      substr(DT_3$停止轉換迄日, 8, 9))


DT_3 <- DT_3 %>%
  select(-file_path, -reason)
names(DT_3)[1] <- '公告日'
names(DT_3)[2] <- '可轉債名稱'
names(DT_3)[3] <- '可轉債代號'

write.csv(DT_3, '../data/CB_report/可轉債停止轉換_20210512.csv')


