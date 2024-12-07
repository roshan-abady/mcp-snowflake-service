# MCP Server for Snowflake / Snowflake MCP 服务器

This is an MCP server implementation for interacting with Snowflake databases.
这是一个用于与 Snowflake 数据库交互的 MCP 服务器实现。

## Installation / 安装

1. Clone this repository / 克隆此仓库
2. Install dependencies / 安装依赖项:
```bash
pip install -e .
```

## Configuration / 配置

Create a `.env` file with your Snowflake credentials:
创建一个包含 Snowflake 凭证的 `.env` 文件：

```
SNOWFLAKE_USER=your_username      # 您的用户名
SNOWFLAKE_PASSWORD=your_password  # 您的密码
SNOWFLAKE_ACCOUNT=your_account    # 您的账户
SNOWFLAKE_DATABASE=your_database  # 您的数据库
SNOWFLAKE_WAREHOUSE=your_warehouse # 您的数据仓库
```

## Usage / 使用方法

Start the server / 启动服务器:
```bash
python -m server.py
```

The server will start listening for MCP client connections.
服务器将开始监听 MCP 客户端连接。