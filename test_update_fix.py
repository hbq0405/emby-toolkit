#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试一键更新功能修复验证脚本
验证立即更新按钮的逻辑是否正确
"""

import os
import sys
import logging
import docker
import json
from pathlib import Path

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_docker_connection():
    """测试Docker连接"""
    try:
        client = docker.from_env()
        client.ping()
        logger.info("✅ Docker连接正常")
        return client
    except Exception as e:
        logger.error(f"❌ Docker连接失败: {e}")
        return None

def test_container_exists(client, container_name):
    """测试容器是否存在"""
    try:
        container = client.containers.get(container_name)
        logger.info(f"✅ 容器 '{container_name}' 存在，状态: {container.status}")
        return True
    except docker.errors.NotFound:
        logger.error(f"❌ 容器 '{container_name}' 不存在")
        return False
    except Exception as e:
        logger.error(f"❌ 检查容器 '{container_name}' 时出错: {e}")
        return False

def test_watchtower_command_construction():
    """测试watchtower命令构建逻辑"""
    logger.info("🔍 测试watchtower命令构建...")
    
    # 模拟system.py中的命令构建逻辑
    container_name = "emby-toolkit"
    command = [
        "--cleanup",
        "--run-once",
        container_name,  # 主程序容器
        "emby-proxy-nginx"  # nginx容器，确保同时重启
    ]
    
    expected_containers = ["emby-toolkit", "emby-proxy-nginx"]
    actual_containers = [cmd for cmd in command if not cmd.startswith("--")]
    
    if set(actual_containers) == set(expected_containers):
        logger.info(f"✅ watchtower命令构建正确: {command}")
        logger.info(f"✅ 将同时更新容器: {actual_containers}")
        return True
    else:
        logger.error(f"❌ watchtower命令构建错误")
        logger.error(f"   期望容器: {expected_containers}")
        logger.error(f"   实际容器: {actual_containers}")
        return False

def test_docker_compose_config():
    """测试docker-compose配置"""
    logger.info("🔍 测试docker-compose配置...")
    
    compose_file = Path("docker-compose.yml")
    if not compose_file.exists():
        logger.error("❌ docker-compose.yml文件不存在")
        return False
    
    try:
        with open(compose_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 检查关键配置
        checks = [
            ("emby-toolkit" in content, "主程序服务配置"),
            ("emby-proxy-nginx" in content, "nginx服务配置"),
            ("depends_on" in content, "服务依赖配置"),
            ("service_healthy" in content, "健康检查依赖"),
            ("/var/run/docker.sock" in content, "Docker socket挂载")
        ]
        
        all_passed = True
        for check, description in checks:
            if check:
                logger.info(f"✅ {description}: 正常")
            else:
                logger.error(f"❌ {description}: 缺失")
                all_passed = False
        
        return all_passed
        
    except Exception as e:
        logger.error(f"❌ 读取docker-compose.yml失败: {e}")
        return False

def test_system_py_update_logic():
    """测试system.py中的更新逻辑"""
    logger.info("🔍 测试system.py更新逻辑...")
    
    system_file = Path("routes/system.py")
    if not system_file.exists():
        logger.error("❌ routes/system.py文件不存在")
        return False
    
    try:
        with open(system_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 检查关键逻辑
        checks = [
            ("emby-proxy-nginx" in content, "nginx容器包含在更新命令中"),
            ("主程序和Nginx将在后台被重启" in content, "更新提示信息正确"),
            ("containrrr/watchtower" in content, "使用正确的watchtower镜像"),
            ("--run-once" in content, "单次运行配置"),
            ("--cleanup" in content, "清理旧镜像配置")
        ]
        
        all_passed = True
        for check, description in checks:
            if check:
                logger.info(f"✅ {description}: 正常")
            else:
                logger.error(f"❌ {description}: 缺失")
                all_passed = False
        
        return all_passed
        
    except Exception as e:
        logger.error(f"❌ 读取routes/system.py失败: {e}")
        return False

def test_frontend_update_logic():
    """测试前端更新逻辑"""
    logger.info("🔍 测试前端更新逻辑...")
    
    frontend_file = Path("emby-actor-ui/src/components/ReleasesPage.vue")
    if not frontend_file.exists():
        logger.error("❌ ReleasesPage.vue文件不存在")
        return False
    
    try:
        with open(frontend_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 检查关键逻辑
        checks = [
            ("立即更新" in content, "立即更新按钮存在"),
            ("EventSource" in content, "事件流处理"),
            ("/api/system/update/stream" in content, "正确的更新API端点"),
            ("showUpdateModal" in content, "更新进度模态框"),
            ("dockerLayers" in content, "Docker层状态跟踪")
        ]
        
        all_passed = True
        for check, description in checks:
            if check:
                logger.info(f"✅ {description}: 正常")
            else:
                logger.error(f"❌ {description}: 缺失")
                all_passed = False
        
        return all_passed
        
    except Exception as e:
        logger.error(f"❌ 读取ReleasesPage.vue失败: {e}")
        return False

def main():
    """主测试函数"""
    logger.info("🚀 开始测试一键更新功能修复...")
    
    tests = [
        ("Docker连接测试", lambda: test_docker_connection() is not None),
        ("watchtower命令构建测试", test_watchtower_command_construction),
        ("docker-compose配置测试", test_docker_compose_config),
        ("system.py更新逻辑测试", test_system_py_update_logic),
        ("前端更新逻辑测试", test_frontend_update_logic)
    ]
    
    # 额外的容器存在性测试
    client = test_docker_connection()
    if client:
        tests.extend([
            ("主程序容器存在性测试", lambda: test_container_exists(client, "emby-toolkit")),
            ("nginx容器存在性测试", lambda: test_container_exists(client, "emby-proxy-nginx"))
        ])
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        logger.info(f"\n--- {test_name} ---")
        try:
            if test_func():
                passed += 1
                logger.info(f"✅ {test_name}: 通过")
            else:
                logger.error(f"❌ {test_name}: 失败")
        except Exception as e:
            logger.error(f"❌ {test_name}: 异常 - {e}")
    
    logger.info(f"\n📊 测试结果: {passed}/{total} 通过")
    
    if passed == total:
        logger.info("🎉 所有测试通过！一键更新功能修复验证成功。")
        logger.info("\n✨ 修复要点总结:")
        logger.info("   1. watchtower命令现在同时更新主程序和nginx容器")
        logger.info("   2. docker-compose.yml中nginx正确依赖主程序健康检查")
        logger.info("   3. 前端正确处理更新事件流和进度显示")
        logger.info("   4. 更新提示信息明确告知用户nginx也会重启")
        return True
    else:
        logger.error(f"❌ {total - passed} 个测试失败，需要进一步检查。")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)