"""
Qiskit 推理脚本：加载 PennyLane 导出的权重，用 Qiskit 一比一复刻 4 比特、6 层 Data Re-uploading 线路。
支持天衍-176（祖冲之2号）物理超导量子计算机。

门映射：
  - PennyLane AngleEmbedding(..., rotation='Y') → circuit.ry(angle, qubit)
  - PennyLane Rot(phi, theta, omega) → RZ(phi) RY(theta) RZ(omega)
  - PennyLane CNOT(wires=[c,t]) → circuit.cx(c, t)

依赖: pip install qiskit qiskit-aer numpy cqlib-adapter
天衍物理机: 设置 TIANYAN_API_TOKEN 或 CQLIB_TOKEN；若后端名不是 tianyan-176，设置 TIANYAN_BACKEND=实际code
本地模拟: USE_TIANYAN=0 python qiskit_inference.py
"""
import os
import numpy as np
from qiskit import QuantumCircuit, transpile

# 是否使用天衍-176 物理机（默认 True）；设为 0 使用本地 Aer：USE_TIANYAN=0 python qiskit_inference.py
USE_TIANYAN = os.environ.get("USE_TIANYAN", "1").strip() in ("1", "true", "yes")

# 默认权重文件（与 qrmi_prototype.py 导出路径一致）
DEFAULT_WEIGHTS_PATH = "qrmi_weights.npz"
DEFAULT_SHOTS = 1024
# 天衍后端名称，可通过环境变量 TIANYAN_BACKEND 覆盖（实际名称以控制台/API 返回的 code 为准）
TIANYAN_BACKEND_NAME = os.environ.get("TIANYAN_BACKEND", "tianyan-176")


def get_backend():
    """
    返回当前使用的后端。
    - 天衍-176：需安装 cqlib-adapter，并设置环境变量 TIANYAN_API_TOKEN。
    - 本地：USE_TIANYAN=0 时使用 AerSimulator。
    参见《天衍实验室使用指南》2.2.2 后端设备接入。
    """
    if USE_TIANYAN:
        try:
            from cqlib_adapter.qiskit_ext.tianyan_provider import TianYanProvider
        except ModuleNotFoundError as e:
            import sys
            raise ModuleNotFoundError(
                "当前使用的 Python 环境中未找到 cqlib_adapter。\n"
                f"当前 Python: {sys.executable}\n"
                "解决方式二选一：\n"
                "  1) 在当前环境安装: pip install cqlib-adapter\n"
                "  2) 用已安装 adapter 的虚拟环境运行，例如:\n"
                f"     /Users/howiesun/project/neurodb/.venv/bin/python qiskit_inference.py\n"
                "     或先 cd 到项目根目录再 source .venv/bin/activate，然后运行本脚本。"
            ) from e

        api_token = (
            os.environ.get("TIANYAN_API_TOKEN", "").strip()
            or os.environ.get("CQLIB_TOKEN", "").strip()
        )
        if not api_token:
            raise ValueError(
                "使用天衍物理机需设置环境变量 TIANYAN_API_TOKEN 或 CQLIB_TOKEN（在控制台获取）。"
                "仅本地测试可执行: USE_TIANYAN=0 python qiskit_inference.py"
            )
        provider = TianYanProvider(token=api_token)
        try:
            backend = provider.backend(TIANYAN_BACKEND_NAME)
        except Exception as e:
            if "not found" in str(e).lower():
                try:
                    backends = provider.backends()
                    names = [b.name for b in backends]
                    raise ValueError(
                        f"未找到后端 '{TIANYAN_BACKEND_NAME}'。\n"
                        f"当前可用后端（name）: {names}\n"
                        "请设置环境变量 TIANYAN_BACKEND=实际后端名称 后重试，或查阅天衍控制台确认后端 code。"
                    ) from e
                except ValueError:
                    raise
                except Exception:
                    pass
            raise
        return backend

    from qiskit_aer import AerSimulator
    return AerSimulator()


def load_weights(path=DEFAULT_WEIGHTS_PATH):
    """从 npz 加载量子权重、经典线性层参数与归一化元数据。"""
    data = np.load(path, allow_pickle=False)
    return {
        "quantum_weights": data["quantum_weights"],  # (n_layers, n_qubits, 3)
        "linear_weight": data["linear_weight"],      # (1, 1)
        "linear_bias": data["linear_bias"],          # (1,)
        "key_min": float(data["key_min"]),
        "key_max": float(data["key_max"]),
        "n_keys": int(data["n_keys"]),
        "n_qubits": int(data["n_qubits"]),
        "n_layers": int(data["n_layers"]),
    }


def key_to_angle(key: float, key_min: float, key_max: float) -> float:
    """将 Key 归一化到 [0, pi]，与 PennyLane 训练时一致。"""
    return (key - key_min) / (key_max - key_min + 1e-9) * np.pi


def build_circuit(angle: float, weights: np.ndarray, n_qubits: int, n_layers: int) -> QuantumCircuit:
    """
    构建单次推理的静态线路（Data Re-uploading）。
    物理机需要：n_qubits 个量子比特 + 1 个经典比特（存第 0 个量子比特的测量结果）。
    angle: 当前 key 对应的编码角度
    weights: (n_layers, n_qubits, 3)，PennyLane Rot(phi, theta, omega) = RZ(phi) RY(theta) RZ(omega)
    """
    # 创建 n_qubits 个量子比特和 1 个经典比特（用来存第 0 个量子比特的结果）
    qc = QuantumCircuit(n_qubits, 1)

    for l in range(n_layers):
        # 1. 数据重上传：AngleEmbedding(rotation='Y') → RY(angle) 于每根线
        for q in range(n_qubits):
            qc.ry(angle, q)

        # 2. 参数化旋转：Rot(phi, theta, omega) = RZ(phi) RY(theta) RZ(omega)
        for q in range(n_qubits):
            phi, theta, omega = weights[l, q, 0], weights[l, q, 1], weights[l, q, 2]
            qc.rz(phi, q)
            qc.ry(theta, q)
            qc.rz(omega, q)

        # 3. 纠缠层：环形 CNOT
        for q in range(n_qubits):
            qc.cx(q, (q + 1) % n_qubits)

    # 测量第 0 个量子比特，结果存入第 0 个经典比特（物理机必须）
    qc.measure(0, 0)
    return qc


def expect_z0_from_shots(circuit: QuantumCircuit, backend, shots: int = DEFAULT_SHOTS) -> float:
    """
    通过 Shots 采样统计得到 <Z0> = P(0) - P(1)，与物理机行为一致。
    编译线路 → 运行 shots 次 → 用计数计算期望值。
    """
    transpiled = transpile(circuit, backend)
    job = backend.run(transpiled, shots=shots)
    result = job.result()
    counts = result.get_counts(transpiled)
    # 仅测量了 1 个经典比特，counts 形如 {'0': n0, '1': n1}
    count_0 = counts.get("0", 0)
    count_1 = counts.get("1", 0)
    z_expectation = (count_0 - count_1) / shots
    return float(z_expectation)


def predict_position(
    key: float,
    quantum_weights: np.ndarray,
    linear_weight: np.ndarray,
    linear_bias: np.ndarray,
    key_min: float,
    key_max: float,
    n_keys: int,
    n_qubits: int,
    n_layers: int,
    backend,
    shots: int = DEFAULT_SHOTS,
) -> float:
    """
    对单个 key 做推理：Key → 角度 → 量子线路 → 测量 + Shots 得 <Z0> → 经典线性层 → 物理位置。
    """
    angle = key_to_angle(key, key_min, key_max)
    qc = build_circuit(angle, quantum_weights, n_qubits, n_layers)
    z_expectation = expect_z0_from_shots(qc, backend, shots)
    # 经典线性层：y_norm = linear_weight * <Z0> + linear_bias
    y_norm = float(linear_weight.flat[0] * z_expectation + linear_bias.flat[0])
    # 反归一化到 [0, n_keys-1] 的物理数组索引
    y_pos = (y_norm + 1) / 2 * (n_keys - 1)
    return max(0, min(n_keys - 1, y_pos))


def run_inference(
    weights_path: str = DEFAULT_WEIGHTS_PATH,
    keys: np.ndarray = None,
    backend=None,
    shots: int = DEFAULT_SHOTS,
):
    """
    加载权重并对给定 keys 做推理；若 keys 为 None 则用导出时的 key 范围生成若干测试点。
    backend 为 None 时使用 get_backend()；物理机接入时传入 TianyanBackend。
    """
    w = load_weights(weights_path)
    n_qubits = w["n_qubits"]
    n_layers = w["n_layers"]
    key_min, key_max, n_keys = w["key_min"], w["key_max"], w["n_keys"]

    if backend is None:
        backend = get_backend()
    if keys is None:
        keys = np.linspace(key_min, key_max, 10)

    backend_label = TIANYAN_BACKEND_NAME + " (物理机)" if USE_TIANYAN else "AerSimulator (本地)"
    print(f"后端: {backend_label}")
    print(f"加载权重: {weights_path} (n_qubits={n_qubits}, n_layers={n_layers}, shots={shots})")
    positions = []
    for key in keys:
        pos = predict_position(
            float(key),
            w["quantum_weights"],
            w["linear_weight"],
            w["linear_bias"],
            key_min, key_max, n_keys,
            n_qubits, n_layers,
            backend=backend,
            shots=shots,
        )
        positions.append(pos)
    return np.array(keys), np.array(positions)


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_WEIGHTS_PATH
    keys, positions = run_inference(path)
    print("Key -> 预测位置 (Qiskit 推理):")
    for k, p in zip(keys, positions):
        print(f"  {k:.1f} -> {p:.2f}")
