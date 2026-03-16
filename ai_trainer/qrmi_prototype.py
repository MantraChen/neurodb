"""
Quantum RMI Prototype: 用 PennyLane + PyTorch 验证「量子态预测键值物理位置」概念。

依赖: pip install pennylane torch numpy matplotlib
"""
import pennylane as qml
import torch
import torch.nn as nn
import numpy as np
import matplotlib.pyplot as plt

# ---------------------------------------------------------
# 1. 数据准备 (模拟数据库中的一段连续 Sorted Keys)
# ---------------------------------------------------------
N_KEYS = 200
# 随机生成一些递增的键值，模拟真实数据分布的稀疏与密集
np.random.seed(42)
raw_keys = np.sort(np.random.choice(range(0, 10000), N_KEYS, replace=False))
positions = np.arange(N_KEYS)

# 量子编码需要归一化：将 Key 映射到 [0, pi] 作为旋转角度
key_min, key_max = raw_keys.min(), raw_keys.max()
X_norm = (raw_keys - key_min) / (key_max - key_min + 1e-9) * np.pi

# 将目标位置映射到 [-1, 1]，因为量子比特 PauliZ 测量的期望值范围是 [-1, 1]
Y_norm = (positions / (N_KEYS - 1)) * 2 - 1

X_tensor = torch.tensor(X_norm, dtype=torch.float32).view(-1, 1)
Y_tensor = torch.tensor(Y_norm, dtype=torch.float32).view(-1, 1)

# ---------------------------------------------------------
# 2. 定义参数化量子线路 (PQC) - 使用 Data Re-uploading 架构
# ---------------------------------------------------------
n_qubits = 4  # 提升到 4 个量子比特
n_layers = 6  # 线路深度提升到 6
dev = qml.device("default.qubit", wires=n_qubits)


@qml.qnode(dev, interface="torch")
def quantum_net(inputs, weights):
    # weights 的形状将会是 (n_layers, n_qubits, 3)
    for l in range(n_layers):
        # 1. 数据重上传：每一层都将特征重新编码到量子态中
        qml.AngleEmbedding(inputs.repeat(1, n_qubits), wires=range(n_qubits), rotation='Y')

        # 2. 参数化旋转门：模型需要学习的参数
        for q in range(n_qubits):
            qml.Rot(*weights[l, q], wires=q)

        # 3. 纠缠层：构建多量子比特之间的关联 (环形 CNOT)
        for q in range(n_qubits):
            qml.CNOT(wires=[q, (q + 1) % n_qubits])

    return qml.expval(qml.PauliZ(0))


# ---------------------------------------------------------
# 3. 封装为 PyTorch 模型 (混合经典-量子架构)
# ---------------------------------------------------------
class QuantumRMI(nn.Module):
    def __init__(self):
        super().__init__()
        weight_shapes = {"weights": (n_layers, n_qubits, 3)}

        # 【关键修复 1：近零初始化】
        # 让所有旋转门初始值极小，使量子线路初始状态接近“恒等变换”，保留梯度
        init_fn = {"weights": lambda x: torch.nn.init.normal_(x, mean=0.0, std=0.05)}
        self.qlayer = qml.qnn.TorchLayer(quantum_net, weight_shapes, init_method=init_fn)

        # 【关键修复 2：经典线性辅助层】
        # 量子线路擅长拟合复杂的“形状”(非线性特征)，但很难完美触达 -1 和 1 的极值边界。
        # 加一个简单的经典线性层作为“放大器/平移器”，让经典网络干苦力，量子网络做特征映射。
        self.linear = nn.Linear(1, 1)

    def forward(self, x):
        # 量子层提取高维干涉特征，经典层负责拉伸对齐
        qout = self.qlayer(x)
        # TorchLayer 可能返回 (1, batch) 或 (batch,)；Linear(1,1) 需要 (batch, 1)
        if qout.dim() == 1:
            qout = qout.unsqueeze(1)
        elif qout.size(0) == 1:
            qout = qout.T
        return self.linear(qout)


# ---------------------------------------------------------
# 4. 训练模型
# ---------------------------------------------------------
def main():
    n_params = n_layers * n_qubits * 3
    print(f"配置: n_qubits={n_qubits}, n_layers={n_layers}, Data Re-uploading 架构, 参数量={n_params}")

    model = QuantumRMI()
    # 学习率可以适当稍微调大，因为我们有了经典线性层的缓冲
    optimizer = torch.optim.Adam(model.parameters(), lr=0.1)
    loss_fn = nn.MSELoss()

    print("开始训练 Quantum RMI 模型 (带近零初始化与经典辅助层)...")
    epochs = 200  # 迭代次数稍微加一点
    for epoch in range(epochs):
        optimizer.zero_grad()
        predictions = model(X_tensor)
        loss = loss_fn(predictions, Y_tensor)
        loss.backward()
        optimizer.step()

        if (epoch + 1) % 50 == 0:
            print(f"Epoch {epoch+1}/{epochs} | Loss: {loss.item():.4f}")

    # ---------------------------------------------------------
    # 5. 评估与误差边界计算 (NeuroDB 回退搜索的核心)
    # ---------------------------------------------------------
    with torch.no_grad():
        y_pred_norm = model(X_tensor).numpy().flatten()

    # 将 [-1, 1] 的期望值映射回 [0, N_KEYS - 1] 的物理数组索引
    y_pred_pos = (y_pred_norm + 1) / 2 * (N_KEYS - 1)
    y_pred_pos_int = np.round(y_pred_pos).astype(int)

    # 计算绝对误差
    errors = positions - y_pred_pos_int
    min_err = int(np.min(errors))
    max_err = int(np.max(errors))

    print("\n--- 训练完成 ---")
    print(f"模型参数量: {n_params} 个旋转角度")
    print(f"物理位置误差边界: MinErr = {min_err}, MaxErr = {max_err}")
    print(f"最大回退搜索范围: {max_err - min_err} 个元素 (占比 {(max_err - min_err)/N_KEYS * 100:.1f}%)")

    # ---------------------------------------------------------
    # 6. 可视化拟合结果
    # ---------------------------------------------------------
    plt.figure(figsize=(10, 5))
    plt.scatter(raw_keys, positions, label="Actual CDF (True Position)", color="blue", s=10)
    plt.plot(raw_keys, y_pred_pos, label="Quantum Predicted Position", color="red", linewidth=2)
    plt.fill_between(raw_keys, y_pred_pos + min_err, y_pred_pos + max_err, color='red', alpha=0.2, label="Error Bound")
    plt.xlabel("Key")
    plt.ylabel("Array Position")
    plt.title("Quantum RMI: Fitting CDF with Parameterized Quantum Circuit")
    plt.legend()
    plt.grid(True)
    plt.show()

    # ---------------------------------------------------------
    # 7. 剥离并导出权重，供 Qiskit 推理使用
    # ---------------------------------------------------------
    export_weights(model, key_min, key_max, N_KEYS)


def export_weights(model, key_min, key_max, n_keys, path="qrmi_weights.npz"):
    """导出量子旋转权重与经典线性层参数，保存为 NumPy 与 JSON 可用的格式。"""
    # 量子层权重: (n_layers, n_qubits, 3)
    quantum_weights = None
    for name, p in model.named_parameters():
        if list(p.shape) == [n_layers, n_qubits, 3]:
            quantum_weights = p.detach().numpy().copy()
            break
    assert quantum_weights is not None, "未找到量子权重 (n_layers, n_qubits, 3)"

    # 经典线性层: y = linear_weight * x + linear_bias
    linear_weight = model.linear.weight.detach().numpy().copy()   # (1, 1)
    linear_bias = model.linear.bias.detach().numpy().copy()         # (1,)

    np.savez(
        path,
        quantum_weights=quantum_weights,
        linear_weight=linear_weight,
        linear_bias=linear_bias,
        key_min=np.array(key_min, dtype=np.float64),
        key_max=np.array(key_max, dtype=np.float64),
        n_keys=np.array(n_keys, dtype=np.int64),
        n_qubits=np.array(n_qubits, dtype=np.int64),
        n_layers=np.array(n_layers, dtype=np.int64),
    )
    print(f"\n权重已导出: {path}")

    # 同时导出 JSON 可读的经典层与元数据（量子权重体积大，仅放 NPZ）
    import json
    meta = {
        "linear_weight": linear_weight.tolist(),
        "linear_bias": linear_bias.tolist(),
        "key_min": float(key_min),
        "key_max": float(key_max),
        "n_keys": int(n_keys),
        "n_qubits": int(n_qubits),
        "n_layers": int(n_layers),
    }
    json_path = path.replace(".npz", "_meta.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    print(f"元数据已导出: {json_path}")


if __name__ == "__main__":
    main()
