# Quant-finance-challenges
A repository dedicated to challenges in quantitative finance, market modelling and high-performance systems (HFT) in C++ and Python.



# 📈 Quantitative Finance & HFT Challenges

A personal repository focused on solving advanced quantitative finance problems, high-frequency trading (HFT) algorithms, ultra-low-latency optimized data structures, and financial market mathematical modeling.

## 🚀 About the Repository

The goal of this space is to document the implementation of robust algorithms, market simulations, and technical challenges inspired by real-world *Trading Technology* and *Quantitative Research* environments. The codebase prioritizes memory efficiency, cache locality, concurrent programming, and *zero dynamic memory allocation* on the critical path.

## 🛠️ Technologies & Tools

* **Languages:** C++ (C++17/20), Python
* **Concepts & Paradigms:** 
  * Lock-free data structures and concurrent programming (Atomics, SeqLocks)
  * Efficient order book management and processing
  * Handling out-of-order and gapped market data streams
  * Quantitative modeling and financial time-series statistical analysis

## 📂 Repository Structure

```text
├── c++ /
│   ├── order_book/          # Lock-free and low-latency order book implementations
│   └── matching_engine/     # Order matching engines
├── python /
│   ├── quantitative_models/ # Pricing models and risk analysis
│   └── market_simulation/   # Order flow simulations and backtesting
└── docs/                    # Notes, roadmaps, and theoretical references
