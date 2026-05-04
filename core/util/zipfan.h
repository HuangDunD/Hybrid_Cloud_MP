#pragma once

#include "cassert"
#include "cmath"
#include "cstdio"
#include <chrono>

#include "util/fast_random.h"

inline long unsigned int GetCPUCycle() {
    // 使用高精度时钟获取纳秒级时间戳来模拟 CPU 周期
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::high_resolution_clock::now().time_since_epoch()
    ).count();
}

class ZipFanGen{
public:
    ZipFanGen(uint64_t n , double theta , uint64_t rand_seed){
        assert(n > 0);
        if (theta > 0.992 && theta < 1)
        fprintf(stderr, "warning: theta > 0.992 will be inaccurate due to approximation\n");
      if (theta >= 1. && theta < 40.) {
        fprintf(stderr, "error: theta in [1., 40.) is not supported\n");
        assert(false);
        theta_ = 0;  // unused
        alpha_ = 0;  // unused
        thres_ = 0;  // unused
        return;
      }
      assert(theta == -1. || (theta >= 0. && theta < 1.) || theta >= 40.);
      n_ = n;
      theta_ = theta;
      if (theta == -1.) {
        seq_ = rand_seed % n;
        alpha_ = 0;  // unused
        thres_ = 0;  // unused
      } else if (theta > 0. && theta < 1.) {
        seq_ = 0;  // unused
        alpha_ = 1. / (1. - theta);
        thres_ = 1. + pow_approx(0.5, theta);
      } else {
        seq_ = 0;     // unused
        alpha_ = 0.;  // unused
        thres_ = 0.;  // unused
      }
      last_n_ = 0;
      zetan_ = 0.;
      eta_ = 0;
      // rand_state_[0] = (unsigned short)(rand_seed >> 0);
      // rand_state_[1] = (unsigned short)(rand_seed >> 16);
      // rand_state_[2] = (unsigned short)(rand_seed >> 32);
      rand_ = Rand(rand_seed);
    }

    ZipFanGen(const ZipFanGen& src) {
        n_ = src.n_;
        theta_ = src.theta_;
        alpha_ = src.alpha_;
        thres_ = src.thres_;
        last_n_ = src.last_n_;
        dbl_n_ = src.dbl_n_;
        zetan_ = src.zetan_;
        eta_ = src.eta_;
        seq_ = src.seq_;
        rand_ = src.rand_;
    }

    ZipFanGen(const ZipFanGen& src, uint64_t rand_seed) {
        n_ = src.n_;
        theta_ = src.theta_;
        alpha_ = src.alpha_;
        thres_ = src.thres_;
        last_n_ = src.last_n_;
        dbl_n_ = src.dbl_n_;
        zetan_ = src.zetan_;
        eta_ = src.eta_;
        seq_ = src.seq_;
        rand_ = Rand(rand_seed);
    }

    ZipFanGen& operator=(const ZipFanGen& src) {
        n_ = src.n_;
        theta_ = src.theta_;
        alpha_ = src.alpha_;
        thres_ = src.thres_;
        last_n_ = src.last_n_;
        dbl_n_ = src.dbl_n_;
        zetan_ = src.zetan_;
        eta_ = src.eta_;
        seq_ = src.seq_;
        rand_ = src.rand_;
        return *this;
    }

    void change_n(uint64_t value) {
        n_ = value;
    }

    // 暴露 zipfian 自身的两个核心参数，便于外部按 (n, theta) 推导热点区间
    double theta() const { return theta_; }
    uint64_t n() const { return n_; }

    // 基于 Zipfian 自身性质推导「头部热点 key 数量」K：
    //   返回最少的 K，使得索引落在 [0, K) 内的累计访问概率 >= mass。
    // 推导：theta∈(0,1) 时 ζ_N(θ)≈N^{1−θ}/(1−θ)，部分和 Σ_{i=1..K} 1/i^θ ≈ K^{1−θ}/(1−θ)，
    //       由 K^{1−θ} ≥ mass · N^{1−θ} 得 K = ceil(N · mass^{1/(1-θ)})。
    // 边界：
    //   theta == -1 (sequential)、theta == 0 (uniform)：无偏斜，返回 0（不视为热点 key）
    //   theta >= 40：always idx 0，返回 1
    //   mass <= 0：返回 0；mass >= 1：返回 n
    static uint64_t HotKeyCount(uint64_t n, double theta, double mass) {
        if (n == 0) return 0;
        if (mass <= 0.0) return 0;
        if (mass >= 1.0) return n;
        if (theta == -1.0) return 0;
        if (theta <= 0.0) return 0;          // 均匀分布：不存在自然热点
        if (theta >= 40.0) return 1;         // 退化为 always 0
        if (theta >= 1.0) {                  // 不支持区间，保守返回 1
            return 1;
        }
        double exponent = 1.0 / (1.0 - theta);
        // 对于 theta 接近 1，exponent 很大，mass^exponent 可能下溢到 0
        double frac = std::pow(mass, exponent);
        if (!(frac > 0.0)) return 1;         // 包含 NaN / 0 / 下溢
        double k = std::ceil((double)n * frac);
        if (k < 1.0) k = 1.0;
        if (k > (double)n) k = (double)n;
        return (uint64_t)k;
    }

    uint64_t next() {
        if (last_n_ != n_) {
          if (theta_ > 0. && theta_ < 1.) {
            zetan_ = zeta(last_n_, zetan_, n_, theta_);
            eta_ = (1. - pow_approx(2. / (double)n_, 1. - theta_)) /
                   (1. - zeta(0, 0., 2, theta_) / zetan_);
          }
          last_n_ = n_;
          dbl_n_ = (double)n_;
        }
    
        if (theta_ == -1.) {
          uint64_t v = seq_;
          if (++seq_ >= n_) seq_ = 0;
          return v;
        } else if (theta_ == 0.) {
          double u = rand_.next_f64();
          return (uint64_t)(dbl_n_ * u);
        } else if (theta_ >= 40.) {
          return 0UL;
        } else {
          // from J. Gray et al. Quickly generating billion-record synthetic
          // databases. In SIGMOD, 1994.
    
          // double u = erand48(rand_state_);
          double u = rand_.next_f64();
          double uz = u * zetan_;
          if (uz < 1.)
            return 0UL;
          else if (uz < thres_)
            return 1UL;
          else {
            uint64_t v =
                (uint64_t)(dbl_n_ * pow_approx(eta_ * (u - 1.) + 1., alpha_));
            if (v >= n_) v = n_ - 1;
            return v;
          }
        }
      }


      static void test(double theta) {
        double zetan = 0.;
        const uint64_t n = 1000000UL;
        uint64_t i;
    
        for (i = 0; i < n; i++) zetan += 1. / pow((double)i + 1., theta);
    
        if (theta < 1. || theta >= 40.) {
        ZipFanGen zg(n, theta, 0);
    
          uint64_t num_key0 = 0;
          const uint64_t num_samples = 10000000UL;
          if (theta < 1. || theta >= 40.) {
            for (i = 0; i < num_samples; i++)
              if (zg.next() == 0) num_key0++;
          }
    
          printf("theta = %lf; using pow(): %.10lf", theta, 1. / zetan);
          if (theta < 1. || theta >= 40.)
            printf(", using approx-pow(): %.10lf",
                   (double)num_key0 / (double)num_samples);
          printf("\n");
        }
      }
    



private:
    // 用来计算类似的 a 的 b 次方，不准确，但是速度快
    static double pow_approx(double a , double b){
        int e = (int)b;
        union {
            double d;
            int x[2];
        } u = {a};
        u.x[1] = (int)((b - (double)e) * (double)(u.x[1] - 1072632447) + 1072632447.);
        u.x[0] = 0;

        // exponentiation by squaring with the exponent's integer part
        // double r = u.d makes everything much slower, not sure why
        // TODO: use popcount?
        double r = 1.;
        while (e) {
            if (e & 1) {
                r *= a;
            }
            a *= a;
            e >>= 1;
        }

        return r * u.d;
    }

    static double zeta(uint64_t last_n , double last_sum , uint64_t n , double theta){
        if (last_n > n){
            last_n = 0;
            last_sum = 0.;
        }
        while (last_n < n){
            last_sum += 1. / pow_approx((double)last_n + 1., theta);
            last_n++;
        }
        return last_sum;
    }

private:
    uint64_t n_;
    double theta_;
    double alpha_;
    double thres_;
    uint64_t last_n_;
    double dbl_n_;
    double zetan_;
    double eta_;
    uint64_t seq_;
    Rand rand_;
}__attribute__((aligned(128)));;

