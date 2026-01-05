#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <algorithm>
#include <vector>
#include <unordered_map>
#include <iomanip>

using namespace std;

vector<string> split(const string& s) {
    vector<string> tokens;
    string token;
    stringstream ss(s);
    while (getline(ss, token, '|'))
        tokens.push_back(token);
    return tokens;
}
// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], string& r_name, string& start_date,string& end_date, int& num_threads,string& table_path, string& result_path) {
// TODO: Implement command line argument parsing
// Example: --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 4 --table_path /path/to/tables --result_path /path/to/results
    for (int i = 1; i < argc; i++) {
        string arg = argv[i];
        if (arg == "--r_name") r_name = argv[++i];
        else if (arg == "--start_date") start_date = argv[++i];
        else if (arg == "--end_date") end_date = argv[++i];
        else if (arg == "--threads") num_threads = stoi(argv[++i]);
        else if (arg == "--table_path") table_path = argv[++i];
        else if (arg == "--result_path") result_path = argv[++i];
    }
    return !r_name.empty() && !start_date.empty() && !end_date.empty()
           && num_threads > 0 && !table_path.empty() && !result_path.empty();
}

bool readTable(const string& file,
               vector<map<string,string>>& data,
               const vector<string>& cols) {

    ifstream f(file);
    if (!f.is_open()) return false;

    string line;
    while (getline(f, line)) {
        auto t = split(line);
        map<string,string> row;
        for (int i = 0; i < cols.size(); i++)
            row[cols[i]] = t[i];
        data.push_back(row);
    }
    return true;
}
// Function to read TPCH data from the specified paths
bool readTPCHData(const string& table_path,vector<map<string,string>>& customer_data,vector<map<string,string>>& orders_data,vector<map<string,string>>& lineitem_data,vector<map<string,string>>& supplier_data,vector<map<string,string>>& nation_data,vector<map<string,string>>& region_data) {
    // TODO: Implement reading TPCH data from files

    return
        readTable(table_path + "/customer.tbl", customer_data,
                  {"c_custkey","c_name","c_address","c_nationkey"}) &&
        readTable(table_path + "/orders.tbl", orders_data,
                  {"o_orderkey","o_custkey","o_status","o_totalprice","o_orderdate"}) &&
        readTable(table_path + "/lineitem.tbl", lineitem_data,
                  {"l_orderkey","l_partkey","l_suppkey","l_linenumber",
                   "l_quantity","l_extendedprice","l_discount"}) &&
        readTable(table_path + "/supplier.tbl", supplier_data,
                  {"s_suppkey","s_name","s_address","s_nationkey"}) &&
        readTable(table_path + "/nation.tbl", nation_data,
                  {"n_nationkey","n_name","n_regionkey"}) &&
        readTable(table_path + "/region.tbl", region_data,
                  {"r_regionkey","r_name"});
}

void worker(int start, int end,
            const vector<map<string,string>>& lineitem_data,
            const unordered_map<string,string>& order_to_cust,
            const unordered_map<string,string>& cust_to_nation,
            const unordered_map<string,string>& supp_to_nation,
            const unordered_map<string,string>& nation_to_name,
            const unordered_map<string,bool>& valid_orders,
            map<string,double>& local_result) {

    for (int i = start; i < end; i++) {
        const auto& li = lineitem_data[i];

        auto itOrder = valid_orders.find(li.at("l_orderkey"));
        if (itOrder == valid_orders.end()) continue;

        auto itCust = order_to_cust.find(li.at("l_orderkey"));
        if (itCust == order_to_cust.end()) continue;

        auto itCustNat = cust_to_nation.find(itCust->second);
        auto itSuppNat = supp_to_nation.find(li.at("l_suppkey"));
        if (itCustNat == cust_to_nation.end() ||
            itSuppNat == supp_to_nation.end()) continue;

        if (itCustNat->second != itSuppNat->second) continue;

        auto itNationName = nation_to_name.find(itSuppNat->second);
        if (itNationName == nation_to_name.end()) continue;

        double price = stod(li.at("l_extendedprice"));
        double discount = stod(li.at("l_discount"));
        local_result[itNationName->second] += price * (1.0 - discount);
    }
}
// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const string& r_name, const string& start_date,const string& end_date, int num_threads,const vector<map<string,string>>& customer_data,const vector<map<string,string>>& orders_data,const vector<map<string,string>>& lineitem_data,const vector<map<string,string>>& supplier_data,const vector<map<string,string>>& nation_data,const vector<map<string,string>>& region_data,map<string,double>& results) {
// TODO: Implement TPCH Query 5 using multithreading
    string regionkey;
    for (auto& r : region_data)
        if (r.at("r_name") == r_name)
            regionkey = r.at("r_regionkey");

    unordered_map<string,string> nation_to_name;
    for (auto& n : nation_data)
        if (n.at("n_regionkey") == regionkey)
            nation_to_name[n.at("n_nationkey")] = n.at("n_name");

    unordered_map<string,string> supp_to_nation;
    for (auto& s : supplier_data)
        supp_to_nation[s.at("s_suppkey")] = s.at("s_nationkey");

    unordered_map<string,string> cust_to_nation;
    for (auto& c : customer_data)
        cust_to_nation[c.at("c_custkey")] = c.at("c_nationkey");

    unordered_map<string,string> order_to_cust;
    unordered_map<string,bool> valid_orders;
    for (auto& o : orders_data) {
        string d = o.at("o_orderdate");
        if (d >= start_date && d < end_date) {
            order_to_cust[o.at("o_orderkey")] = o.at("o_custkey");
            valid_orders[o.at("o_orderkey")] = true;
        }
    }

    vector<thread> threads;
    vector<map<string,double>> locals(num_threads);
    int chunk = lineitem_data.size() / num_threads;

    for (int i = 0; i < num_threads; i++) {
        int s = i * chunk;
        int e = (i == num_threads - 1) ? lineitem_data.size() : s + chunk;
        threads.emplace_back(worker, s, e,
                             cref(lineitem_data),
                             cref(order_to_cust),
                             cref(cust_to_nation),
                             cref(supp_to_nation),
                             cref(nation_to_name),
                             cref(valid_orders),
                             ref(locals[i]));
    }

    for (auto& t : threads) t.join();

    for (auto& m : locals)
        for (auto& p : m)
            results[p.first] += p.second;

    return true;
}
// Function to output results to the specified path
bool outputResults(const string& result_path,const map<string,double>& results) {
// TODO: Implement outputting results to a file
    ofstream out(result_path);
    if (!out.is_open()) return false;
    out << std::fixed << std::setprecision(2);
    for (auto& r : results)
        out << r.first << "|" << r.second << "\n";
    return true;
}
