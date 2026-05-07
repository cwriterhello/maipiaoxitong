# 基础讲解-如何安装ngrok



**1 输入地址**

[ngrok | Unified Application Delivery Platform for Developers](https://ngrok.com/)



**2 点击 Get ngrok**

![1719478307543-768ff1eb-8370-4d05-b4a8-5231382fee30.png](./img/itVlFf_566jfZ9PX/1719478307543-768ff1eb-8370-4d05-b4a8-5231382fee30-725624.png)



**3 选择下载平台，我选择的是windows**

![1719478354365-5a8f0c6e-064e-454a-83f0-06bb71d5c276.png](./img/itVlFf_566jfZ9PX/1719478354365-5a8f0c6e-064e-454a-83f0-06bb71d5c276-299837.png)



**4 下载后得到压缩文件，然后进行解压，得到ngrok.exe文件**

![1719478630900-b78d40d1-058c-40f2-907c-022cf16f62aa.png](./img/itVlFf_566jfZ9PX/1719478630900-b78d40d1-058c-40f2-907c-022cf16f62aa-479034.png)

![1719478742908-6a3d6971-665d-4d73-bec2-5d47dc4cbcec.png](./img/itVlFf_566jfZ9PX/1719478742908-6a3d6971-665d-4d73-bec2-5d47dc4cbcec-478928.png)



**5 启动该文件**

![1719478825042-282af026-da08-4395-8f96-6acdee8676a4.png](./img/itVlFf_566jfZ9PX/1719478825042-282af026-da08-4395-8f96-6acdee8676a4-226153.png)



**6 接下来需要注册一个ngrok的账号来获取属于你的密钥**

![1719478952226-aa5ee929-10d1-4687-a050-14e309d629a3.png](./img/itVlFf_566jfZ9PX/1719478952226-aa5ee929-10d1-4687-a050-14e309d629a3-874944.png)

可以使用你的github账号登录

![1719479007664-5ae55a9e-3193-48e4-9b1e-a21bf0cb580d.png](./img/itVlFf_566jfZ9PX/1719479007664-5ae55a9e-3193-48e4-9b1e-a21bf0cb580d-814237.png)



**7 获取到秘钥**

![1719479381287-cecd6e6e-1a96-46f5-896b-d71a10892dd2.png](./img/itVlFf_566jfZ9PX/1719479381287-cecd6e6e-1a96-46f5-896b-d71a10892dd2-145500.png)



**8 执行命令**

```shell
ngrok config add-authtoken xxxxx
```

xxxx 替换成你自己的秘钥



**9 执行成功后，命令行界面中会出现下面的信息。此时，代表配置成功。ngrok程序已经在你的用户目录下，创建一个.ngrok2文件夹，并在文件夹中创建一个配置文件ngrok.yml**

![1719479561527-56891c39-a5ab-4d32-a51c-e9b7b11f82f4.png](./img/itVlFf_566jfZ9PX/1719479561527-56891c39-a5ab-4d32-a51c-e9b7b11f82f4-687302.png)



**10 在命令行界面中，执行下面命令，即将本地端口映射到外网中，我这里映射的是 6085，如果需要映射其他端口，只需改成相对应的端口即可**

```shell
ngrok http 6085
```



**11 执行后会出现映射后的页面，说明启动成功**

该程序需一直保持运行，程序关闭，映射也将关闭。如果需要关闭映射，可以使用ctrl + c 或关闭该界面，进行程序终止。每次重新执行命令，映射外网的域名都会发生改变。如果希望域名不变，可通过开通ngrok的会员服务，具体可在官网进行查看

![1719479873308-6ef9c110-f392-49f0-9dcd-2ab9518c1212.png](./img/itVlFf_566jfZ9PX/1719479873308-6ef9c110-f392-49f0-9dcd-2ab9518c1212-838291.png)



### 另外一款内网穿透工具
#### 飞鸽
地址：[https://www.fgnwct.com/index.html](https://www.fgnwct.com/index.html)

这款是国内的内网穿透工具，使用起来比较简单，

缺点就是要花钱，实名认证2元+开通内网穿透6.9元每月 = 8.9元



> 更新: 2024-07-24 17:38:42  
> 原文: <https://www.yuque.com/u22210564/ykdrdh/bwmy16g2tz6h5sec>