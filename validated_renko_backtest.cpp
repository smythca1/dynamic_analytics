#include <algorithm>
#include <cctype>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace fs = std::filesystem;

struct Args {
    std::string archive_root;
    std::string symbol = "TSLA";
    std::string output_dir;
    std::string source_mode = "1s-close";
    double min_pct = 0.10;
    double max_pct = 1.00;
    double step_pct = 0.01;
    std::vector<double> fixed_brick_sizes;
    std::vector<int> entries{1};
    std::vector<int> tps{1, 2};
    std::vector<int> sls{1, 2};
    double slippage_per_fill = 0.15;
    double min_neighbor_profitability = 70.0;
    double max_trades_per_session = 100.0;
    std::string entry_session_start_utc = "13:30:00";
    std::string entry_session_end_utc = "19:59:00";
    std::string force_flat_utc = "19:59:00";
    bool raw_duplicates = true;
    bool write_bricks = false;
};

struct PriceLoad {
    std::vector<double> prices;
    std::vector<std::string> timestamps;
    std::vector<std::string> dates;
    std::map<std::string, long long> raw_rows_by_date;
    long long raw_rows = 0;
    long long skipped_unchanged = 0;
    std::string first_ts;
    std::string last_ts;
};

struct Accumulator {
    int entry = 1;
    int tp = 1;
    int sl = 1;
    int side = 0;
    double entry_price = 0.0;
    double take_profit = 0.0;
    double stop_loss = 0.0;
    long long trades = 0;
    long long wins = 0;
    long long losses = 0;
    long long flat = 0;
    long long force_flat_exits = 0;
    double gross = 0.0;
    double net = 0.0;
    double gross_compound = 1.0;
    double equity = 0.0;
    double peak = 0.0;
    double max_dd = 0.0;

    void close_position(double exit_price, double slippage, bool force_flat_exit) {
        if (side == 0) return;
        const double gross_pnl = (exit_price - entry_price) * side;
        const double net_pnl = gross_pnl - (2.0 * slippage);
        if (entry_price > 0.0) gross_compound *= 1.0 + (gross_pnl / entry_price);
        trades++;
        if (net_pnl > 0.0) wins++;
        else if (net_pnl < 0.0) losses++;
        else flat++;
        if (force_flat_exit) force_flat_exits++;
        gross += gross_pnl;
        net += net_pnl;
        equity += net_pnl;
        peak = std::max(peak, equity);
        max_dd = std::max(max_dd, peak - equity);
        side = 0;
        entry_price = 0.0;
        take_profit = 0.0;
        stop_loss = 0.0;
    }

    void force_flat(double price, double slippage) {
        close_position(price, slippage, true);
    }

    void on_brick(double close, int direction, int run_len, double brick_size, double slippage, bool allow_new_entry) {
        bool exited_on_this_brick = false;
        if (side != 0) {
            bool exit_hit = false;
            constexpr double epsilon = 1e-6;
            if (side == 1) {
                exit_hit = close >= take_profit - epsilon || close <= stop_loss + epsilon;
            } else {
                exit_hit = close <= take_profit + epsilon || close >= stop_loss - epsilon;
            }
            if (exit_hit) {
                close_position(close, slippage, false);
                exited_on_this_brick = true;
            }
        }

        if (side == 0) {
            if (!allow_new_entry) return;
            if (exited_on_this_brick) return;
            if (run_len != entry) return;
            side = direction;
            entry_price = close;
            take_profit = entry_price + side * tp * brick_size;
            stop_loss = entry_price - side * sl * brick_size;
            return;
        }
    }
};

struct Result {
    std::string symbol;
    double ltp = 0.0;
    double pct = 0.0;
    double brick = 0.0;
    int entry = 1;
    int tp = 1;
    int sl = 1;
    long long trades = 0;
    long long wins = 0;
    long long losses = 0;
    long long flat = 0;
    double win_rate = 0.0;
    double gross = 0.0;
    double net = 0.0;
    double avg = 0.0;
    double gross_fixed_return = 0.0;
    double fixed_return = 0.0;
    double gross_compounded_return = 0.0;
    double max_dd = 0.0;
    bool open = false;
    long long bricks = 0;
    double neighbor_profitability = 0.0;
    int neighbor_count = 0;
    double trades_per_session = 0.0;
    std::string validation_flags;
};

struct BrickAudit {
    long long index = 0;
    std::string timestamp;
    double tick_price = 0.0;
    double previous_close = 0.0;
    double brick_close = 0.0;
    int direction = 0;
    int run_len = 0;
    double threshold = 0.0;
};

struct RenkoParityArgs {
    bool enabled = false;
    std::string input_csv;
    double brick_size = 1.0;
    std::string anchor_mode = "first_price";
    double anchor_price = 0.0;
};

struct LogicalBrick {
    std::string timestamp;
    double close = 0.0;
    int direction = 0;
    int run_len = 0;
};

struct RenkoParityState {
    double brick_size = 1.0;
    double last_close = 0.0;
    int run_dir = 0;
    int run_len = 0;
    bool initialized = false;
    std::string anchor_mode = "first_price";
    double anchor_price = 0.0;
};

constexpr double RENKO_EPSILON = 1e-12;

static std::string arg_value(int& idx, int argc, char** argv);

static std::vector<std::string> split(const std::string& raw, char delim) {
    std::vector<std::string> out;
    std::stringstream ss(raw);
    std::string item;
    while (std::getline(ss, item, delim)) {
        if (!item.empty()) out.push_back(item);
    }
    return out;
}

static std::vector<int> parse_ints(const std::string& raw) {
    std::vector<int> values;
    for (const auto& part : split(raw, ',')) values.push_back(std::stoi(part));
    if (values.empty()) throw std::runtime_error("empty integer list");
    return values;
}

static std::vector<double> parse_doubles(const std::string& raw) {
    std::vector<double> values;
    for (const auto& part : split(raw, ',')) values.push_back(std::stod(part));
    if (values.empty()) throw std::runtime_error("empty double list");
    return values;
}

static std::string trim_copy(const std::string& raw) {
    size_t start = 0;
    while (start < raw.size() && std::isspace(static_cast<unsigned char>(raw[start]))) start++;
    size_t end = raw.size();
    while (end > start && std::isspace(static_cast<unsigned char>(raw[end - 1]))) end--;
    return raw.substr(start, end - start);
}

static bool threshold_gte(double value, double threshold) {
    return value >= threshold - RENKO_EPSILON;
}

static bool threshold_lte(double value, double threshold) {
    return value <= threshold + RENKO_EPSILON;
}

static double initial_renko_close_cpp(double price, const RenkoParityState& state) {
    std::string mode = state.anchor_mode;
    std::transform(mode.begin(), mode.end(), mode.begin(), [](unsigned char c) { return std::tolower(c); });
    if (mode == "first_price" || mode == "first" || mode == "price") return price;
    if (mode == "grid" || mode == "zero_grid" || mode == "anchored_grid") {
        return state.anchor_price + std::floor((price - state.anchor_price) / state.brick_size) * state.brick_size;
    }
    throw std::runtime_error("unsupported parity anchor mode: " + state.anchor_mode);
}

static bool next_renko_step_cpp(const RenkoParityState& state, double price, int& direction, int& close_bricks) {
    const double move = price - state.last_close;
    direction = 0;
    close_bricks = 1;
    if (state.run_dir == 1) {
        if (threshold_gte(move, state.brick_size)) {
            direction = 1;
            close_bricks = 1;
            return true;
        }
        if (threshold_lte(move, -2.0 * state.brick_size)) {
            direction = -1;
            close_bricks = 2;
            return true;
        }
    } else if (state.run_dir == -1) {
        if (threshold_lte(move, -state.brick_size)) {
            direction = -1;
            close_bricks = 1;
            return true;
        }
        if (threshold_gte(move, 2.0 * state.brick_size)) {
            direction = 1;
            close_bricks = 2;
            return true;
        }
    } else {
        if (threshold_gte(move, state.brick_size)) {
            direction = 1;
            close_bricks = 1;
            return true;
        }
        if (threshold_lte(move, -state.brick_size)) {
            direction = -1;
            close_bricks = 1;
            return true;
        }
    }
    return false;
}

static void update_renko_parity_state(
    RenkoParityState& state,
    const std::string& timestamp,
    double price,
    std::vector<LogicalBrick>& bricks
) {
    if (state.brick_size <= 0.0) throw std::runtime_error("brick_size must be > 0");
    if (!state.initialized) {
        state.last_close = initial_renko_close_cpp(price, state);
        state.initialized = true;
        return;
    }
    while (true) {
        int direction = 0;
        int close_bricks = 1;
        if (!next_renko_step_cpp(state, price, direction, close_bricks)) break;
        state.last_close += direction * state.brick_size * close_bricks;
        if (direction == state.run_dir) {
            state.run_len++;
        } else {
            state.run_dir = direction;
            state.run_len = 1;
        }
        bricks.push_back(LogicalBrick{timestamp, state.last_close, direction, state.run_len});
    }
}

static RenkoParityArgs parse_renko_parity_args(int argc, char** argv) {
    RenkoParityArgs out;
    for (int i = 1; i < argc; i++) {
        const std::string key = argv[i];
        if (key == "--renko-parity-csv") {
            out.enabled = true;
            out.input_csv = arg_value(i, argc, argv);
        } else if (key == "--brick-size") {
            out.brick_size = std::stod(arg_value(i, argc, argv));
        } else if (key == "--anchor-mode") {
            out.anchor_mode = arg_value(i, argc, argv);
        } else if (key == "--anchor-price") {
            out.anchor_price = std::stod(arg_value(i, argc, argv));
        }
    }
    if (out.enabled && out.input_csv.empty()) throw std::runtime_error("--renko-parity-csv requires a path");
    return out;
}

static int run_renko_parity_mode(const RenkoParityArgs& args) {
    std::ifstream in(args.input_csv);
    if (!in) throw std::runtime_error("could not open parity csv: " + args.input_csv);
    std::string line;
    std::getline(in, line);
    RenkoParityState state;
    state.brick_size = args.brick_size;
    state.anchor_mode = args.anchor_mode;
    state.anchor_price = args.anchor_price;
    std::vector<LogicalBrick> bricks;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        const auto parts = split(line, ',');
        if (parts.size() < 2) continue;
        const std::string ts = trim_copy(parts[0]);
        const double price = std::stod(trim_copy(parts[1]));
        update_renko_parity_state(state, ts, price, bricks);
    }
    std::cout << "timestamp,brick_close,direction,run_len\n";
    std::cout << std::fixed << std::setprecision(12);
    for (const auto& brick : bricks) {
        std::cout << brick.timestamp << ',' << brick.close << ',' << brick.direction << ',' << brick.run_len << "\n";
    }
    return 0;
}

static std::string arg_value(int& idx, int argc, char** argv) {
    if (idx + 1 >= argc) throw std::runtime_error(std::string("missing value for ") + argv[idx]);
    return argv[++idx];
}

static Args parse_args(int argc, char** argv) {
    Args args;
    for (int i = 1; i < argc; i++) {
        const std::string key = argv[i];
        if (key == "--archive-root") args.archive_root = arg_value(i, argc, argv);
        else if (key == "--symbol") args.symbol = arg_value(i, argc, argv);
        else if (key == "--output-dir") args.output_dir = arg_value(i, argc, argv);
        else if (key == "--source-mode") args.source_mode = arg_value(i, argc, argv);
        else if (key == "--min-pct") args.min_pct = std::stod(arg_value(i, argc, argv));
        else if (key == "--max-pct") args.max_pct = std::stod(arg_value(i, argc, argv));
        else if (key == "--step-pct") args.step_pct = std::stod(arg_value(i, argc, argv));
        else if (key == "--brick-sizes") args.fixed_brick_sizes = parse_doubles(arg_value(i, argc, argv));
        else if (key == "--entry-bricks") args.entries = parse_ints(arg_value(i, argc, argv));
        else if (key == "--take-profit-bricks") args.tps = parse_ints(arg_value(i, argc, argv));
        else if (key == "--stop-loss-bricks") args.sls = parse_ints(arg_value(i, argc, argv));
        else if (key == "--slippage-per-fill") args.slippage_per_fill = std::stod(arg_value(i, argc, argv));
        else if (key == "--min-neighbor-profitability") args.min_neighbor_profitability = std::stod(arg_value(i, argc, argv));
        else if (key == "--max-trades-per-session") args.max_trades_per_session = std::stod(arg_value(i, argc, argv));
        else if (key == "--entry-session-start-utc") args.entry_session_start_utc = arg_value(i, argc, argv);
        else if (key == "--entry-session-end-utc") args.entry_session_end_utc = arg_value(i, argc, argv);
        else if (key == "--force-flat-utc") args.force_flat_utc = arg_value(i, argc, argv);
        else if (key == "--write-bricks") args.write_bricks = true;
        else if (key == "--dedupe") args.raw_duplicates = false;
        else throw std::runtime_error("unknown arg: " + key);
    }
    if (args.archive_root.empty()) throw std::runtime_error("--archive-root required");
    if (args.output_dir.empty()) throw std::runtime_error("--output-dir required");
    if (args.source_mode != "1s-close") {
        throw std::runtime_error("--source-mode must be canonical 1s-close; raw tick mode is noncanonical and blocked");
    }
    return args;
}

static bool extract_json_string(const std::string& line, const std::string& key, std::string& out) {
    const std::string needle = "\"" + key + "\":";
    size_t pos = line.find(needle);
    if (pos == std::string::npos) return false;
    pos = line.find('"', pos + needle.size());
    if (pos == std::string::npos) return false;
    size_t end = line.find('"', pos + 1);
    if (end == std::string::npos) return false;
    out = line.substr(pos + 1, end - pos - 1);
    return true;
}

static bool extract_json_double(const std::string& line, const std::string& key, double& out) {
    const std::string needle = "\"" + key + "\":";
    size_t pos = line.find(needle);
    if (pos == std::string::npos) return false;
    pos += needle.size();
    while (pos < line.size() && (line[pos] == ' ' || line[pos] == '"')) pos++;
    size_t end = pos;
    while (end < line.size() && (std::isdigit(line[end]) || line[end] == '.' || line[end] == '-')) end++;
    try {
        out = std::stod(line.substr(pos, end - pos));
        return out > 0.0;
    } catch (...) {
        return false;
    }
}

static std::vector<std::string> discover_dates(const Args& args) {
    std::vector<std::string> dates;
    const fs::path ticks_root = fs::path(args.archive_root) / "ticks";
    for (const auto& entry : fs::directory_iterator(ticks_root)) {
        if (!entry.is_directory()) continue;
        const fs::path tick_file = entry.path() / (args.symbol + ".jsonl");
        if (fs::exists(tick_file)) dates.push_back(entry.path().filename().string());
    }
    std::sort(dates.begin(), dates.end());
    return dates;
}

static std::string timestamp_second_bucket(const std::string& ts, const std::string& fallback_date) {
    if (ts.size() >= 19 && ts[4] == '-' && ts[7] == '-' && (ts[10] == 'T' || ts[10] == ' ')) {
        return ts.substr(0, 19);
    }
    return fallback_date;
}

static void append_price_point(PriceLoad& load, const std::string& ts, double price, bool skip_unchanged) {
    if (skip_unchanged && !load.prices.empty() && price == load.prices.back()) {
        load.skipped_unchanged++;
        return;
    }
    load.prices.push_back(price);
    load.timestamps.push_back(ts);
}

static PriceLoad load_tick_prices(const Args& args) {
    PriceLoad load;
    load.dates = discover_dates(args);
    for (const auto& date : load.dates) {
        const fs::path path = fs::path(args.archive_root) / "ticks" / date / (args.symbol + ".jsonl");
        std::ifstream in(path);
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            double price = 0.0;
            std::string ts;
            if (!extract_json_double(line, "price", price)) continue;
            if (!extract_json_string(line, "timestamp", ts)) ts = date;
            load.raw_rows++;
            load.raw_rows_by_date[date]++;
            if (load.first_ts.empty()) load.first_ts = ts;
            load.last_ts = ts;
            append_price_point(load, ts, price, true);
        }
    }
    return load;
}

static PriceLoad load_1s_close_prices(const Args& args) {
    PriceLoad load;
    load.dates = discover_dates(args);
    std::string current_bucket;
    double current_close = 0.0;
    bool has_bucket = false;

    auto flush_bucket = [&]() {
        if (!has_bucket) return;
        append_price_point(load, current_bucket, current_close, false);
        has_bucket = false;
        current_bucket.clear();
    };

    for (const auto& date : load.dates) {
        const fs::path path = fs::path(args.archive_root) / "ticks" / date / (args.symbol + ".jsonl");
        std::ifstream in(path);
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            double price = 0.0;
            std::string ts;
            if (!extract_json_double(line, "price", price)) continue;
            if (!extract_json_string(line, "timestamp", ts)) ts = date;
            load.raw_rows++;
            load.raw_rows_by_date[date]++;
            if (load.first_ts.empty()) load.first_ts = ts;
            load.last_ts = ts;

            const std::string bucket = timestamp_second_bucket(ts, date);
            if (!has_bucket) {
                current_bucket = bucket;
                current_close = price;
                has_bucket = true;
                continue;
            }
            if (bucket != current_bucket) {
                flush_bucket();
                current_bucket = bucket;
                current_close = price;
                has_bucket = true;
            } else {
                current_close = price;
            }
        }
    }
    flush_bucket();
    return load;
}

static PriceLoad load_prices(const Args& args) {
    if (args.source_mode == "tick") return load_tick_prices(args);
    return load_1s_close_prices(args);
}

static std::vector<double> pct_grid(const Args& args) {
    std::vector<double> values;
    for (double p = args.min_pct; p <= args.max_pct + 1e-12; p += args.step_pct) {
        values.push_back(std::round(p * 1000000.0) / 1000000.0);
    }
    return values;
}

static std::vector<double> brick_grid(const Args& args, double ltp) {
    if (!args.fixed_brick_sizes.empty()) return args.fixed_brick_sizes;
    std::vector<double> bricks;
    for (double pct : pct_grid(args)) bricks.push_back(ltp * (pct / 100.0));
    return bricks;
}

static double grid_anchor(double price, double brick) {
    return std::floor(price / brick) * brick;
}

static std::string timestamp_hhmmss(const std::string& ts) {
    if (ts.size() >= 19 && (ts[10] == 'T' || ts[10] == ' ')) return ts.substr(11, 8);
    if (ts.size() >= 8) return ts.substr(0, 8);
    return "";
}

static std::string timestamp_date(const std::string& ts) {
    if (ts.size() >= 10 && ts[4] == '-' && ts[7] == '-') return ts.substr(0, 10);
    return "";
}

static bool entry_allowed_at(const Args& args, const std::string& ts) {
    if (args.entry_session_start_utc == "all" || args.entry_session_end_utc == "all") return true;
    const std::string t = timestamp_hhmmss(ts);
    if (t.empty()) return false;
    if (args.entry_session_start_utc <= args.entry_session_end_utc) {
        return t >= args.entry_session_start_utc && t < args.entry_session_end_utc;
    }
    return t >= args.entry_session_start_utc || t < args.entry_session_end_utc;
}

static bool force_flat_due_at(const Args& args, const std::string& ts) {
    if (args.force_flat_utc.empty() || args.force_flat_utc == "none" || args.force_flat_utc == "off") return false;
    const std::string t = timestamp_hhmmss(ts);
    if (t.empty()) return false;
    return t >= args.force_flat_utc;
}

static std::vector<Result> simulate_brick_size(
    const Args& args,
    const PriceLoad& load,
    double ltp,
    double brick,
    std::vector<BrickAudit>* brick_audit
) {
    const auto& prices = load.prices;
    const double pct = ltp ? (brick / ltp * 100.0) : 0.0;
    std::vector<Accumulator> accs;
    for (int e : args.entries) for (int tp : args.tps) for (int sl : args.sls) {
        Accumulator acc;
        acc.entry = e; acc.tp = tp; acc.sl = sl;
        accs.push_back(acc);
    }
    double last_close = grid_anchor(prices.front(), brick);
    int run_dir = 0;
    int run_len = 0;
    long long bricks = 0;
    std::string force_flat_date;
    for (size_t i = 1; i < prices.size(); i++) {
        const std::string current_date = timestamp_date(load.timestamps[i]);
        if (!current_date.empty() && force_flat_date != current_date && force_flat_due_at(args, load.timestamps[i])) {
            for (auto& acc : accs) {
                acc.force_flat(prices[i], args.slippage_per_fill);
            }
            force_flat_date = current_date;
        }

        while (true) {
            const double move = prices[i] - last_close;
            int direction = 0;
            int close_bricks = 1;
            double threshold = last_close;
            if (run_dir == 1) {
                if (move >= brick) {
                    direction = 1;
                    threshold = last_close + brick;
                } else if (move <= -2.0 * brick) {
                    direction = -1;
                    close_bricks = 2;
                    threshold = last_close - 2.0 * brick;
                }
            } else if (run_dir == -1) {
                if (move <= -brick) {
                    direction = -1;
                    threshold = last_close - brick;
                } else if (move >= 2.0 * brick) {
                    direction = 1;
                    close_bricks = 2;
                    threshold = last_close + 2.0 * brick;
                }
            } else {
                if (move >= brick) {
                    direction = 1;
                    threshold = last_close + brick;
                } else if (move <= -brick) {
                    direction = -1;
                    threshold = last_close - brick;
                }
            }
            if (direction == 0) break;

            const double previous_close = last_close;
            last_close += direction * brick * close_bricks;
            if (run_dir == direction) run_len++; else { run_dir = direction; run_len = 1; }
            bricks++;
            if (brick_audit != nullptr) {
                brick_audit->push_back(BrickAudit{
                    bricks,
                    load.timestamps[i],
                    prices[i],
                    previous_close,
                    last_close,
                    direction,
                    run_len,
                    threshold,
                });
            }
            const bool allow_new_entry = entry_allowed_at(args, load.timestamps[i]);
            for (auto& acc : accs) {
                acc.on_brick(last_close, direction, run_len, brick, args.slippage_per_fill, allow_new_entry);
            }
        }
    }
    std::vector<Result> results;
    for (const auto& acc : accs) {
        Result r;
        r.symbol = args.symbol;
        r.ltp = ltp; r.pct = pct; r.brick = brick;
        r.entry = acc.entry; r.tp = acc.tp; r.sl = acc.sl;
        r.trades = acc.trades; r.wins = acc.wins; r.losses = acc.losses; r.flat = acc.flat;
        r.win_rate = acc.trades ? (100.0 * acc.wins / acc.trades) : 0.0;
        r.gross = acc.gross; r.net = acc.net;
        r.avg = acc.trades ? acc.net / acc.trades : 0.0;
        r.gross_fixed_return = ltp ? 100.0 * acc.gross / ltp : 0.0;
        r.fixed_return = ltp ? 100.0 * acc.net / ltp : 0.0;
        r.gross_compounded_return = 100.0 * (acc.gross_compound - 1.0);
        r.max_dd = acc.max_dd;
        r.open = acc.side != 0;
        r.bricks = bricks;
        results.push_back(r);
    }
    return results;
}

static void write_brick_audit(const fs::path& path, const std::vector<BrickAudit>& bricks) {
    std::ofstream out(path);
    out << "brick_index,timestamp,tick_price,previous_close,threshold,brick_close,direction,run_len,threshold_breached\n";
    out << std::fixed << std::setprecision(6);
    for (const auto& b : bricks) {
        const bool breached = b.direction == 1 ? b.tick_price >= b.threshold : b.tick_price <= b.threshold;
        out << b.index << ',' << b.timestamp << ',' << b.tick_price << ',' << b.previous_close << ','
            << b.threshold << ',' << b.brick_close << ',' << b.direction << ',' << b.run_len << ','
            << (breached ? "true" : "false") << "\n";
    }
}

static void add_neighbor_metrics(std::vector<Result>& rows, double pct_step) {
    for (auto& row : rows) {
        int count = 0, profitable = 0;
        for (const auto& other : rows) {
            if (&row == &other) continue;
            if (std::abs(other.pct - row.pct) <= pct_step + 1e-9 &&
                std::abs(other.entry - row.entry) <= 1 &&
                std::abs(other.tp - row.tp) <= 1 &&
                std::abs(other.sl - row.sl) <= 1) {
                count++;
                if (other.net > 0.0) profitable++;
            }
        }
        row.neighbor_count = count;
        row.neighbor_profitability = count ? (100.0 * profitable / count) : 0.0;
    }
}

static void add_validation_flags(std::vector<Result>& rows, const Args& args, size_t sessions) {
    const double session_count = sessions == 0 ? 1.0 : static_cast<double>(sessions);
    for (auto& row : rows) {
        row.trades_per_session = row.trades / session_count;
        std::vector<std::string> flags;
        if (row.trades_per_session > args.max_trades_per_session) flags.push_back("HIGH_TRADE_COUNT");
        if (row.neighbor_profitability < args.min_neighbor_profitability) flags.push_back("LOW_NEIGHBOR_PROFITABILITY");
        if (row.open) flags.push_back("OPEN_POSITION");
        if (row.net <= 0.0) flags.push_back("NON_POSITIVE_NET");
        if (flags.empty()) {
            row.validation_flags = "PASS";
        } else {
            std::ostringstream ss;
            for (size_t i = 0; i < flags.size(); i++) {
                if (i) ss << "|";
                ss << flags[i];
            }
            row.validation_flags = ss.str();
        }
    }
}

static void write_csv(const fs::path& path, const std::vector<Result>& rows, double slippage_per_fill, size_t limit = 0) {
    std::ofstream out(path);
    out << "symbol,ltp_reference,brick_pct_of_ltp,brick_size,entry_bricks,take_profit_bricks,stop_loss_bricks,"
        << "renko_bricks,trades,wins,losses,flat,win_rate_pct,gross_pnl_1share,pnl_1share,avg_pnl_1share,"
        << "gross_fixed_return_pct,fixed_return_pct,gross_compounded_return_pct,max_drawdown_1share,"
        << "neighbor_count,neighbor_profitability_pct,trades_per_session,open_position,slippage_per_fill,validation_flags\n";
    const size_t n = limit == 0 ? rows.size() : std::min(limit, rows.size());
    out << std::fixed << std::setprecision(6);
    for (size_t i = 0; i < n; i++) {
        const auto& r = rows[i];
        out << r.symbol << ',' << r.ltp << ',' << r.pct << ',' << r.brick << ','
            << r.entry << ',' << r.tp << ',' << r.sl << ',' << r.bricks << ','
            << r.trades << ',' << r.wins << ',' << r.losses << ',' << r.flat << ','
            << r.win_rate << ',' << r.gross << ',' << r.net << ',' << r.avg << ','
            << r.gross_fixed_return << ',' << r.fixed_return << ',' << r.gross_compounded_return << ','
            << r.max_dd << ',' << r.neighbor_count << ',' << r.neighbor_profitability << ','
            << r.trades_per_session << ',' << (r.open ? "true" : "false") << ','
            << slippage_per_fill << ',' << r.validation_flags << "\n";
    }
}

static std::string cell(double value, int precision = 1) {
    std::ostringstream ss;
    ss << std::fixed << std::setprecision(precision) << value;
    return ss.str();
}

static void write_svg(const fs::path& path, const std::vector<Result>& rows) {
    const size_t n = std::min<size_t>(100, rows.size());
    const int row_h = 28, width = 1120, height = 120 + static_cast<int>(n) * row_h;
    std::ofstream out(path);
    out << "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"" << width << "\" height=\"" << height << "\">";
    out << "<rect width=\"100%\" height=\"100%\" fill=\"#f8faf7\"/>";
    out << "<style>text{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Arial,sans-serif;fill:#1f2937}.h{font-size:12px;font-weight:750}.c{font-size:11px;font-weight:650}.t{font-size:22px;font-weight:750}.s{font-size:12px;fill:#64748b}</style>";
    out << "<text x=\"24\" y=\"32\" class=\"t\">Validated 1S Renko Continuation Sweep</text>";
    out << "<text x=\"24\" y=\"56\" class=\"s\">Ranked by net PnL/share. Includes neighbor profitability and fixed/compounded returns.</text>";
    std::vector<std::pair<std::string,int>> heads = {{"Setup",190},{"PnL $",280},{"Fix %",365},{"GComp %",455},{"Neigh %",550},{"DD $",640},{"Trades",730},{"Win %",815},{"Bricks",900},{"Open",1010}};
    for (auto& h : heads) out << "<text x=\"" << h.second << "\" y=\"88\" text-anchor=\"middle\" class=\"h\">" << h.first << "</text>";
    for (size_t i = 0; i < n; i++) {
        const auto& r = rows[i];
        int y = 116 + static_cast<int>(i) * row_h;
        std::ostringstream setup;
        setup << cell(r.pct, 2) << "% E" << r.entry << " TP" << r.tp << " SL" << r.sl;
        std::vector<std::pair<std::string,int>> vals = {
            {setup.str(),190},{cell(r.net,2),280},{cell(r.fixed_return,1),365},{cell(r.gross_compounded_return,1),455},
            {cell(r.neighbor_profitability,1),550},{cell(r.max_dd,2),640},{std::to_string(r.trades),730},
            {cell(r.win_rate,1),815},{std::to_string(r.bricks),900},{r.open ? "Y" : "N",1010}
        };
        for (auto& v : vals) out << "<text x=\"" << v.second << "\" y=\"" << y << "\" text-anchor=\"middle\" class=\"c\">" << v.first << "</text>";
    }
    out << "</svg>\n";
}

static void write_metadata(const fs::path& path, const Args& args, const PriceLoad& load, double ltp, const std::vector<Result>& rows) {
    long long max_trades = 0;
    long long max_bricks = 0;
    long long trusted = 0;
    for (const auto& r : rows) {
        max_trades = std::max(max_trades, r.trades);
        max_bricks = std::max(max_bricks, r.bricks);
        if (r.validation_flags == "PASS") trusted++;
    }
    std::ofstream out(path);
    out << "{\n";
    out << "  \"method\": \"validated_renko_continuation_cpp_v3_intraday_force_flat\",\n";
    out << "  \"archive_root\": \"" << args.archive_root << "\",\n";
    out << "  \"symbol\": \"" << args.symbol << "\",\n";
    out << "  \"source_mode\": \"" << args.source_mode << "\",\n";
    out << "  \"promotion_eligible\": false,\n";
    out << "  \"promotion_block_reason\": \"C++ historical helper does not consume persisted Silver canonical observations or contract fingerprints\",\n";
    out << "  \"entry_session_start_utc\": \"" << args.entry_session_start_utc << "\",\n";
    out << "  \"entry_session_end_utc\": \"" << args.entry_session_end_utc << "\",\n";
    out << "  \"force_flat_utc\": \"" << args.force_flat_utc << "\",\n";
    out << "  \"sessions\": " << load.dates.size() << ",\n";
    out << "  \"first_date\": \"" << (load.dates.empty() ? "" : load.dates.front()) << "\",\n";
    out << "  \"last_date\": \"" << (load.dates.empty() ? "" : load.dates.back()) << "\",\n";
    out << "  \"first_tick\": \"" << load.first_ts << "\",\n";
    out << "  \"last_tick\": \"" << load.last_ts << "\",\n";
    out << "  \"raw_ticks\": " << load.raw_rows << ",\n";
    out << "  \"source_price_points\": " << load.prices.size() << ",\n";
    out << "  \"skipped_unchanged_prices\": " << load.skipped_unchanged << ",\n";
    out << "  \"ltp_reference\": " << std::fixed << std::setprecision(6) << ltp << ",\n";
    out << "  \"max_trades\": " << max_trades << ",\n";
    out << "  \"max_renko_bricks\": " << max_bricks << ",\n";
    out << "  \"min_neighbor_profitability\": " << args.min_neighbor_profitability << ",\n";
    out << "  \"max_trades_per_session\": " << args.max_trades_per_session << ",\n";
    out << "  \"trusted_candidates\": " << trusted << ",\n";
    out << "  \"validation_status\": \"" << (trusted > 0 ? "PASS_WITH_TRUSTED_CANDIDATES" : "NO_TRUSTED_CANDIDATES") << "\"\n";
    out << "}\n";
}

int main(int argc, char** argv) {
    try {
        const RenkoParityArgs parity_args = parse_renko_parity_args(argc, argv);
        if (parity_args.enabled) {
            return run_renko_parity_mode(parity_args);
        }
        Args args = parse_args(argc, argv);
        fs::create_directories(args.output_dir);
        PriceLoad load = load_prices(args);
        if (load.prices.empty()) throw std::runtime_error("no prices loaded");
        const double ltp = load.prices.back();
        std::vector<Result> rows;
        std::vector<BrickAudit> brick_audit;
        const auto bricks = brick_grid(args, ltp);
        for (double brick : bricks) {
            std::vector<BrickAudit>* audit_ptr = (args.write_bricks && bricks.size() == 1) ? &brick_audit : nullptr;
            auto part = simulate_brick_size(args, load, ltp, brick, audit_ptr);
            rows.insert(rows.end(), part.begin(), part.end());
        }
        add_neighbor_metrics(rows, args.step_pct);
        add_validation_flags(rows, args, load.dates.size());
        std::sort(rows.begin(), rows.end(), [](const Result& a, const Result& b) {
            if (a.net != b.net) return a.net > b.net;
            return a.trades > b.trades;
        });
        std::vector<Result> trusted;
        for (const auto& row : rows) {
            if (row.validation_flags == "PASS") trusted.push_back(row);
        }
        const fs::path out = args.output_dir;
        write_csv(out / "renko_results_full_sweep.csv", rows, args.slippage_per_fill);
        write_csv(out / "top100_scalper_candidates.csv", rows, args.slippage_per_fill, 100);
        write_csv(out / "trusted_candidates.csv", trusted, args.slippage_per_fill);
        write_svg(out / "top100_scalper_heatmap.svg", rows);
        if (args.write_bricks) write_brick_audit(out / "renko_bricks_audit.csv", brick_audit);
        write_metadata(out / "validation.json", args, load, ltp, rows);
        std::cout << "wrote " << out << "\n";
        std::cout << "rows=" << rows.size() << " source_mode=" << args.source_mode
                  << " raw_ticks=" << load.raw_rows << " source_price_points=" << load.prices.size() << "\n";
        std::cout << "best_net=" << rows.front().net << " trades=" << rows.front().trades << " neighbor=" << rows.front().neighbor_profitability << "\n";
        return 0;
    } catch (const std::exception& exc) {
        std::cerr << "error: " << exc.what() << "\n";
        return 1;
    }
}
