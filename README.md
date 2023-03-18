# mongocheckd

> 用来校验两个mongodb库数据的一致性

## 环境部署说明

```bash
# 1. 依赖:
#   python3.11

# 2. 需要python3支持，并且安装好pipenv:
pip3 install pipenv

# 3. 在项目根目录初始化python依赖环境:
pipenv install

# 4. 修改 etc 配置:
#   etc/config.env

# 5. 启动程序
pipenv run python mongocheckd.py
```