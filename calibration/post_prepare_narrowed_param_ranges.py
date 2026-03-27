# ========================================================
# 基于 Cluster 0 (NSE > 0.82) 的锚定参数配置
# 策略: Center ± 10% (对于小值参数增加最小绝对误差保护)
# ========================================================

def get_anchored_bounds(center, pct=0.10, min_buffer=0.0):
    """
    计算锚定边界
    :param center: 聚类中心值
    :param pct: 相对百分比浮动 (默认 10%)
    :param min_buffer: 最小绝对值宽度 (防止接近0的数值范围过窄)
    """
    # 计算相对宽度
    width = abs(center) * pct

    # 如果相对宽度小于最小缓冲，使用最小缓冲
    if width < min_buffer:
        width = min_buffer

    return [round(center - width, 4), round(center + width, 4)]


# === 1. 上游局地参数 (仅作用于上游单元) ===
# 这些参数定义时需指定 'units': upstream_ids

upstream_local_params = [
    # --- 核心产流参数 ---
    # CN2|1: Center -14.35 -> Range [-15.78, -12.91]
    {'name': 'cn2|1', 'bounds': get_anchored_bounds(-14.348, 0.10)},

    # CN2|2: Center 8.83 -> Range [7.95, 9.71]
    {'name': 'cn2|2', 'bounds': get_anchored_bounds(8.829, 0.10)},

    # LatQ_Co: Center 0.79 -> Range [0.71, 0.87] (侧向流核心)
    {'name': 'latq_co', 'bounds': get_anchored_bounds(0.785, 0.10)},

    # ESCO: Center 0.0035.
    # 警告: ±10% 只有 ±0.0003，太窄。给予 ±0.002 的缓冲 -> [0.0015, 0.0055]
    {'name': 'esco', 'bounds': get_anchored_bounds(0.0035, 0.10, min_buffer=0.002)},

    # PERCO: Center 0.03. 给予 ±0.01 的缓冲 -> [0.02, 0.04]
    {'name': 'perco', 'bounds': get_anchored_bounds(0.0299, 0.10, min_buffer=0.01)},

    # EPCO: Center 0.53 -> [0.47, 0.58]
    {'name': 'epco', 'bounds': get_anchored_bounds(0.526, 0.10)},

    # AWC: Center -1.72.
    # 警告: ±0.17 太小。建议给予 ±1.0 的缓冲 -> [-2.72, -0.72]
    {'name': 'awc', 'bounds': get_anchored_bounds(-1.725, 0.10, min_buffer=1.0)},

    # PETCO: Center 0.74 -> [0.67, 0.81]
    {'name': 'petco', 'bounds': get_anchored_bounds(0.741, 0.10)},

    # SP_YLD: Center 0.31 -> [0.28, 0.34]
    {'name': 'sp_yld', 'bounds': get_anchored_bounds(0.309, 0.10)},

    # --- 融雪参数 (温度类参数必须用绝对缓冲) ---
    # Snofall_tmp: 1.97 ± 0.5 -> [1.47, 2.47]
    {'name': 'snofall_tmp', 'bounds': get_anchored_bounds(1.974, 0.10, min_buffer=0.5)},

    # Snomelt_tmp: -0.14 ± 0.5 -> [-0.64, 0.36]
    {'name': 'snomelt_tmp', 'bounds': get_anchored_bounds(-0.145, 0.10, min_buffer=0.5)},

    # Snomelt_lag: 0.86 -> [0.77, 0.95]
    {'name': 'snomelt_lag', 'bounds': get_anchored_bounds(0.862, 0.10)},

    # Max/Min Melt:
    {'name': 'snomelt_max', 'bounds': get_anchored_bounds(3.209, 0.10)},  # [2.89, 3.53]
    {'name': 'snomelt_min', 'bounds': get_anchored_bounds(1.446, 0.10)},  # [1.30, 1.59]

    # --- 汇流微调 (OvN, Canmx) ---
    # OvN: Center -0.40. 必须给大缓冲，否则范围无效。
    {'name': 'ovn', 'bounds': get_anchored_bounds(-0.397, 0.10, min_buffer=2.0)},
    {'name': 'canmx', 'bounds': get_anchored_bounds(6.096, 0.10, min_buffer=1.0)},

    # --- 河道参数 ---
    {'name': 'chk|1', 'bounds': get_anchored_bounds(1.487, 0.10)},
    {'name': 'chk|2', 'bounds': get_anchored_bounds(3.543, 0.10)},
    {'name': 'flo_min', 'bounds': get_anchored_bounds(0.035, 0.10, min_buffer=0.2)},
    {'name': 'dep_bot', 'bounds': get_anchored_bounds(18.57, 0.10)},
]

# === 2. 全局参数 (作用于全流域，需谨慎) ===
# 这些参数通常不需要指定 'units'，或者指定为全流域 ID

global_params = [
    # Surlag: Center 0.63.
    # 策略: 放宽到 ±15% 并确保有绝对宽度，以适应下游更长的汇流时间
    # Range: [0.53, 0.73]
    {'name': 'surlag', 'bounds': get_anchored_bounds(0.628, 0.15, min_buffer=0.05)},

    # Deep Seep: Center 0.023.
    # 策略: 下游地下水情况未知，给予 ±0.015 的缓冲
    # Range: [0.008, 0.038]
    {'name': 'deep_seep', 'bounds': get_anchored_bounds(0.023, 0.10, min_buffer=0.015)}
]

# 打印确认范围
print(">>> 上游参数锁定范围 (Cluster 0 Anchored):")
for p in upstream_local_params + global_params:
    print(f"{p['name']:<12} : {p['bounds']}")