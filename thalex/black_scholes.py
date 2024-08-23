import math

SQRT_PI = math.sqrt(math.pi)
SQRT_2 = math.sqrt(2)
SQRT_PI_2 = SQRT_PI * SQRT_2


class Greeks:
    def __init__(self, delta=None):
        # Thalex gives us the deltas for free, so calculating them is optional
        self.delta = 0 if delta is None else delta
        self.gamma = 0
        self.vega = 0
        self.theta = 0

    def add(self, other):
        self.delta += other.delta
        self.gamma += other.gamma
        self.vega += other.vega
        self.theta += other.theta

    def mult(self, factor: float):
        self.delta *= factor
        self.gamma *= factor
        self.vega *= factor
        self.theta *= factor

    def __repr__(self):
        return f"delta: {self.delta:.2f}, gamma: {self.gamma:.5f}, vega: {self.vega:.2f}, theta: {self.theta:.2f})"


def call_discount(fwd: float, k: int, sigma: float, maturity: float) -> float:
    voltime = math.sqrt(maturity) * sigma
    if voltime > 0.0:
        d1 = math.log(fwd / k) / voltime + 0.5 * voltime
        norm_d1 = 0.5 + 0.5 * math.erf(d1 / math.sqrt(2))
        norm_d1_vol = 0.5 + 0.5 * math.erf((d1 - voltime) / math.sqrt(2))
        return fwd * norm_d1 - k * norm_d1_vol
    elif fwd > k:
        return fwd - k
    else:
        return 0.0


def put_discount(fwd: float, k: int, sigma: float, maturity: float) -> float:
    voltime = math.sqrt(maturity) * sigma
    if voltime > 0.0:
        d1 = math.log(fwd / k) / voltime + 0.5 * voltime
        norm_d1 = 0.5 + 0.5 * math.erf(-d1 / math.sqrt(2))
        norm_d1_vol = 0.5 + 0.5 * math.erf((voltime - d1) / math.sqrt(2))
        return k * norm_d1_vol - fwd * norm_d1
    elif fwd > k:
        return fwd - k
    else:
        return 0.0


def call_delta(fwd: float, k: int, sigma: float, maturity: float) -> float:
    voltime = math.sqrt(maturity) * sigma
    if voltime > 0.0:
        d1 = math.log(fwd / k) / voltime + 0.5 * voltime
        return 0.5 + 0.5 * math.erf(d1 / math.sqrt(2))
    else:
        return 1.0 if fwd > k else 0.0


def put_delta(fwd: float, k: int, sigma: float, maturity: float) -> float:
    return call_delta(fwd, k, sigma, maturity) - 1.0


def gamma(fwd: float, k: float, sigma: float, maturity: float) -> float:
    voltime = math.sqrt(maturity) * sigma
    if voltime > 0.0:
        d1 = math.log(fwd / k) / voltime + 0.5 * voltime
        inc_norm = math.exp(math.pow(d1, 2) / -2.0) / SQRT_PI_2
        return inc_norm / (fwd * voltime)
    else:
        return 0.0


def vega(fwd: float, k: float, sigma: float, maturity: float) -> float:
    voltime = math.sqrt(maturity) * sigma
    if voltime > 0.0:
        d1 = math.log(fwd / k) / voltime + 0.5 * voltime
        inc_norm = math.exp(math.pow(d1, 2) / -2.0) / SQRT_PI_2
        return 0.01 * fwd * inc_norm * voltime / sigma
    else:
        return 0.0


def all_greeks(
    fwd: float, k: float, sigma: float, maturity: float, is_put: bool, delta=None
) -> Greeks:
    voltime = math.sqrt(maturity) * sigma
    greeks = Greeks(delta)
    if voltime > 0.0:
        d1 = math.log(fwd / k) / voltime + 0.5 * voltime
        if delta is None:
            greeks.delta = 0.5 + 0.5 * math.erf(d1 / math.sqrt(2))
            if is_put:
                greeks.delta -= 1.0
        inc_norm = math.exp(math.pow(d1, 2) / -2.0) / SQRT_PI_2
        greeks.gamma = inc_norm / (fwd * voltime)
        greeks.vega = 0.01 * fwd * inc_norm * voltime / sigma
    elif fwd > k:
        greeks.delta = 1.0
    return greeks
