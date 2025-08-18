#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 Docker 权限修复效果的脚本
用于验证容器内是否能正常访问 Docker socket
"""

import docker
import os
import sys

def test_docker_connection():
    """测试 Docker 连接和权限"""
    print("🔍 开始测试 Docker 连接和权限...")
    
    try:
        # 1. 检查 Docker socket 文件是否存在
        docker_sock_path = "/var/run/docker.sock"
        if os.path.exists(docker_sock_path):
            print(f"✅ Docker socket 文件存在: {docker_sock_path}")
            
            # 检查文件权限
            stat_info = os.stat(docker_sock_path)
            print(f"📋 Docker socket 权限: {oct(stat_info.st_mode)[-3:]}")
            print(f"📋 Docker socket 所有者: UID={stat_info.st_uid}, GID={stat_info.st_gid}")
        else:
            print(f"❌ Docker socket 文件不存在: {docker_sock_path}")
            return False
            
        # 2. 测试 Docker 客户端初始化
        print("\n🔧 测试 Docker 客户端初始化...")
        client = docker.from_env()
        print("✅ Docker 客户端初始化成功")
        
        # 3. 测试 Docker API 连接
        print("\n🌐 测试 Docker API 连接...")
        client.ping()
        print("✅ Docker API 连接成功")
        
        # 4. 获取 Docker 版本信息
        print("\n📊 获取 Docker 版本信息...")
        version_info = client.version()
        print(f"✅ Docker 版本: {version_info.get('Version', 'Unknown')}")
        print(f"✅ API 版本: {version_info.get('ApiVersion', 'Unknown')}")
        
        # 5. 测试容器列表获取
        print("\n📦 测试容器列表获取...")
        containers = client.containers.list(all=True)
        print(f"✅ 成功获取容器列表，共 {len(containers)} 个容器")
        
        # 显示当前容器信息
        for container in containers:
            if 'emby-toolkit' in container.name or 'emby-proxy-nginx' in container.name:
                print(f"  📋 发现相关容器: {container.name} (状态: {container.status})")
        
        # 6. 测试镜像列表获取
        print("\n🖼️ 测试镜像列表获取...")
        images = client.images.list()
        print(f"✅ 成功获取镜像列表，共 {len(images)} 个镜像")
        
        print("\n🎉 所有 Docker 权限测试通过！")
        return True
        
    except docker.errors.DockerException as e:
        print(f"❌ Docker 连接错误: {e}")
        if "Permission denied" in str(e):
            print("💡 建议解决方案:")
            print("   1. 确保容器以正确的用户ID运行")
            print("   2. 检查 docker.sock 的权限设置")
            print("   3. 重启容器服务")
        return False
        
    except Exception as e:
        print(f"❌ 未知错误: {e}")
        return False

def test_environment_variables():
    """测试环境变量配置"""
    print("\n🔍 检查环境变量配置...")
    
    required_vars = [
        'CONTAINER_NAME',
        'DOCKER_IMAGE_NAME',
        'APP_DATA_DIR',
        'PUID',
        'PGID'
    ]
    
    for var in required_vars:
        value = os.environ.get(var)
        if value:
            print(f"✅ {var} = {value}")
        else:
            print(f"⚠️ {var} 未设置")
    
    # 检查当前用户信息
    print(f"\n👤 当前进程用户信息:")
    print(f"   UID: {os.getuid()}")
    print(f"   GID: {os.getgid()}")
    print(f"   用户组: {os.getgroups()}")

if __name__ == "__main__":
    print("=" * 60)
    print("🐳 Docker 权限修复测试脚本")
    print("=" * 60)
    
    # 测试环境变量
    test_environment_variables()
    
    # 测试 Docker 连接
    success = test_docker_connection()
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 测试结果: 所有测试通过，Docker 权限配置正常！")
        sys.exit(0)
    else:
        print("❌ 测试结果: Docker 权限配置存在问题，需要进一步排查。")
        sys.exit(1)
    print("=" * 60)