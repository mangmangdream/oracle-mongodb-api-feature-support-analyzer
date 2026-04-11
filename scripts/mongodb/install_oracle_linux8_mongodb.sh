#!/usr/bin/env bash

set -euo pipefail

BASE_DIR="${1:-/u01/app/mongodb}"
MONGO_VERSION="${MONGO_VERSION:-8.0}"
SERVICE_NAME="${SERVICE_NAME:-mongod-test}"
MONGO_USER="${MONGO_USER:-mongod}"
MONGO_GROUP="${MONGO_GROUP:-mongod}"
CONFIG_PATH="${BASE_DIR}/conf/mongod.conf"
SERVICE_PATH="/etc/systemd/system/${SERVICE_NAME}.service"
REPO_PATH="/etc/yum.repos.d/mongodb-org-${MONGO_VERSION}.repo"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "请使用 root 执行该脚本。"
  exit 1
fi

if [[ ! -f /etc/os-release ]]; then
  echo "无法识别当前操作系统。"
  exit 1
fi

source /etc/os-release
if [[ "${ID:-}" != "ol" || "${VERSION_ID%%.*}" != "8" ]]; then
  echo "该脚本仅针对 Oracle Linux 8 编写，当前系统: ${PRETTY_NAME:-unknown}"
  exit 1
fi

KERNEL_INFO="$(uname -r)"
if [[ "${KERNEL_INFO}" == *"uek"* ]]; then
  echo "检测到 UEK 内核: ${KERNEL_INFO}"
  echo "MongoDB 官方仅声明支持 Oracle Linux 8 的 RHCK，请先切换到 Red Hat Compatible Kernel。"
  exit 1
fi

echo "[1/8] 安装依赖工具"
dnf install -y curl policycoreutils-python-utils

echo "[2/8] 写入 MongoDB ${MONGO_VERSION} YUM 源"
cat > "${REPO_PATH}" <<EOF
[mongodb-org-${MONGO_VERSION}]
name=MongoDB Repository
baseurl=https://repo.mongodb.org/yum/redhat/8/mongodb-org/${MONGO_VERSION}/x86_64/
gpgcheck=1
enabled=1
gpgkey=https://pgp.mongodb.com/server-${MONGO_VERSION}.asc
EOF

echo "[3/8] 安装 mongodb-org"
dnf install -y mongodb-org

echo "[4/8] 准备目录 ${BASE_DIR}"
mkdir -p \
  "${BASE_DIR}/bin" \
  "${BASE_DIR}/conf" \
  "${BASE_DIR}/data" \
  "${BASE_DIR}/log" \
  "${BASE_DIR}/run" \
  "${BASE_DIR}/scripts"

if ! getent group "${MONGO_GROUP}" >/dev/null 2>&1; then
  groupadd --system "${MONGO_GROUP}"
fi

if ! id "${MONGO_USER}" >/dev/null 2>&1; then
  useradd --system --gid "${MONGO_GROUP}" --home-dir "${BASE_DIR}" --shell /sbin/nologin "${MONGO_USER}"
fi

chown -R "${MONGO_USER}:${MONGO_GROUP}" "${BASE_DIR}"

echo "[5/8] 写入 mongod.conf"
cat > "${CONFIG_PATH}" <<EOF
systemLog:
  destination: file
  path: ${BASE_DIR}/log/mongod.log
  logAppend: true

storage:
  dbPath: ${BASE_DIR}/data
  journal:
    enabled: true

processManagement:
  fork: false
  pidFilePath: ${BASE_DIR}/run/mongod.pid
  timeZoneInfo: /usr/share/zoneinfo

net:
  port: 27017
  bindIp: 127.0.0.1

setParameter:
  enableLocalhostAuthBypass: false
EOF

echo "[6/8] 写入 systemd 服务 ${SERVICE_NAME}"
cat > "${SERVICE_PATH}" <<EOF
[Unit]
Description=MongoDB Community Test Server
After=network-online.target
Wants=network-online.target

[Service]
User=${MONGO_USER}
Group=${MONGO_GROUP}
Environment="HOME=${BASE_DIR}"
ExecStart=/usr/bin/mongod --config ${CONFIG_PATH}
RuntimeDirectory=${SERVICE_NAME}
RuntimeDirectoryMode=0755
LimitNOFILE=64000
TimeoutStartSec=300
TimeoutStopSec=120
Restart=on-failure
RestartSec=5
PIDFile=${BASE_DIR}/run/mongod.pid

[Install]
WantedBy=multi-user.target
EOF

echo "[7/8] 处理 SELinux 上下文"
if command -v semanage >/dev/null 2>&1; then
  semanage fcontext -a -t mongod_var_lib_t "${BASE_DIR}/data(/.*)?" || true
  semanage fcontext -a -t mongod_log_t "${BASE_DIR}/log(/.*)?" || true
  semanage fcontext -a -t mongod_var_run_t "${BASE_DIR}/run(/.*)?" || true
  restorecon -Rv "${BASE_DIR}/data" "${BASE_DIR}/log" "${BASE_DIR}/run" || true
else
  echo "未找到 semanage，若 SELinux 为 Enforcing，请手工设置上下文。"
fi

echo "[8/8] 启动服务"
systemctl daemon-reload
systemctl enable --now "${SERVICE_NAME}"
systemctl --no-pager --full status "${SERVICE_NAME}"

echo
echo "MongoDB 已安装完成。"
echo "配置文件: ${CONFIG_PATH}"
echo "数据目录: ${BASE_DIR}/data"
echo "日志文件: ${BASE_DIR}/log/mongod.log"
echo "服务名称: ${SERVICE_NAME}"
echo
echo "后续可使用以下命令验证："
echo "  mongosh --host 127.0.0.1 --port 27017 --eval 'db.adminCommand({ ping: 1 })'"
