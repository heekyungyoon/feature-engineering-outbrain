#include <fstream>
#include <iostream>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <unordered_map>
#include <vector>
#include <chrono>

using namespace std;

std::string line;
std::string document_id;
std::string topic_id;
std::string confidence_level;
std::string uuid;
//    std::string timestamp;
//    std::string platform;
//    std::string geo_location;
std::string others;
std::string display_id;

std::unordered_map<std::string, int> uuid_map;

int get_uid(std::string &uuid) {
    int uid;
    auto pair = uuid_map.find(uuid);
    if (pair != uuid_map.end()) {
        uid = pair->second;
    } else {
        uid = uuid_map.size();
        uuid_map.insert(make_pair(uuid, uid));
    }
    return uid;
}


struct pairhash {
public:
  template <typename T, typename U>
  std::size_t operator()(const std::pair<T, U> &x) const
  {
    return std::hash<T>()(x.first) ^ std::hash<U>()(x.second);
  }
};


struct SimpleHash {
    size_t operator()(const std::pair<std::string, int>& p) const {
        return hash<string>{} (p.first) ^ p.second;
    }
};

//std::istream readfile(std::string filepath)
//{
//    std::cout << "Reading file: " << filepath << std::endl;
//
//    ifstream file(filepath, ios_base::in | ios_base::binary);
//    boost::iostreams::filtering_streambuf<boost::iostreams::input> inbuf;
//    inbuf.push(boost::iostreams::gzip_decompressor());
//    inbuf.push(file);
//
//    std::istream instream(&inbuf);
//
//    std::string line;
//    std::getline(instream, line);
//
//    std::cout << line << std::endl;
//
//    return instream;
//}


std::unordered_map<int, std::vector<std::pair<int, float>>> gen_doc_topic_map()
{
    std::unordered_map<int, std::vector<std::pair<int, float>>> doc_topic;
    string filename = "/home/yhk00323/input/documents_topics.csv.gz";

    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    std::cout << "Start processing " << filename << std::endl;

    ifstream topic_file(filename, ios_base::in | ios_base::binary);
    boost::iostreams::filtering_streambuf<boost::iostreams::input> topic_inbuf;
    topic_inbuf.push(boost::iostreams::gzip_decompressor());
    topic_inbuf.push(topic_file);
    std::istream topic_instream(&topic_inbuf);

    std::getline(topic_instream, line);
    std::cout << "  Headers: " << line << std::endl;

    // transform to unordered map


    int i = 0;
    while(std::getline(topic_instream, document_id, ',')) {
        std::getline(topic_instream, topic_id, ',');
        std::getline(topic_instream, confidence_level);

        auto item = doc_topic.find(stoi(document_id));
        if (item != doc_topic.end()) {
            item->second.push_back(make_pair(stoi(topic_id), stof(confidence_level)));
        } else {
            std::vector<std::pair<int, float>> v;
            v.push_back(make_pair(stoi(topic_id), stof(confidence_level)));
            doc_topic.insert({stoi(document_id), v});
        }
        ++i;
    }

    topic_file.close();
    //    for(auto& p: doc_topic) {
//        std::cout << "document_id: " << p.first << std::endl;
//        for (auto& pp: p.second) {
//            std::cout << " topic_id: " <<pp.first << " , confidence_level: " << pp.second << '\n';
//        }
//    }

    std::cout << "\ni = " << i <<"\nTime taken (sec): "
              << std::chrono::duration_cast<std::chrono::seconds>
                      (std::chrono::steady_clock::now() - begin).count()
              << "\n"
              << std::endl;

    return doc_topic;
};


std::unordered_map<std::pair<int, int>, float, pairhash> gen_user_topic_map(
        std::unordered_map<int, std::vector<std::pair<int, float>>> *doc_topic_map)
{
    std::unordered_map<std::pair<int, int>, float, pairhash> user_topic_map;
    string filename = "/home/yhk00323/input/page_views.csv.gz";
    //string filename = "/Users/heekyungyoon/Projects/feature_engineering_outbrain/data/page_views_sample.csv.gz";

    // I. calculate user-topic interaction based on page_views
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    std::cout << "Start processing " << filename << std::endl;

    ifstream file(filename, ios_base::in | ios_base::binary);
    boost::iostreams::filtering_streambuf<boost::iostreams::input> inbuf;
    inbuf.push(boost::iostreams::gzip_decompressor());
    inbuf.push(file);
    std::istream instream(&inbuf);

    // transform to unordered map
    std::getline(instream, line);
    std::cout << "  Headers: " << line << std::endl;

    int i = 0;
    while(std::getline(instream, uuid, ',')) {
        int uid = get_uid(uuid);
        //if (i == 100000000)
        //    break;

        std::getline(instream, document_id, ',');
//        std::getline(instream, timestamp, ',');
//        std::getline(instream, platform, ',');
//        std::getline(instream, geo_location, ',');
        std::getline(instream, others);

        auto document = (*doc_topic_map).find(stoi(document_id));
        if (document != (*doc_topic_map).end()) {
            for (auto &t: document->second) {
                //if user topic exists
                auto user_topic = user_topic_map.find(make_pair(uid, t.first));
                if (user_topic != user_topic_map.end()) {
                    user_topic->second += t.second;
                } //else user topic doesn't exist
                else {
                    user_topic_map.insert({make_pair(uid, t.first), t.second});
                }
            }
        }
        if (i % 1000000 == 0)
            std::cout << i/1000000 << "M... " << std::endl;
        ++i;
    }

//    for(auto& p: user_topic_map) {
//        std::cout << "uuid: " << p.first << std::endl;
//        for (auto& pp: p.second) {
//            std::cout << " topic_id: " <<pp.first << " , confidence_level: " << pp.second << '\n';
//        }
//    }

    //Cleanup
    file.close();

    std::cout << "\ni = " << i <<"\nTime taken (sec): "
              << std::chrono::duration_cast<std::chrono::seconds>
                      (std::chrono::steady_clock::now() - begin).count()
              << "\n"
              << std::endl;

    return user_topic_map;
}


std::unordered_map<int, std::pair<int, int>> gen_display_map()
{
    // read events to get uuid and document id from clicks_train
    std::unordered_map<int, std::pair<int, int>> display_map;
    string filename = "/home/yhk00323/input/events.csv.gz";
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    std::cout << "Start processing " << filename << std::endl;

    ifstream file(filename, ios_base::in | ios_base::binary);
    boost::iostreams::filtering_streambuf<boost::iostreams::input> inbuf;
    inbuf.push(boost::iostreams::gzip_decompressor());
    inbuf.push(file);
    std::istream instream(&inbuf);

    std::getline(instream, line);
    std::cout << "  Headers: " << line << std::endl;

    int i = 0;
    while(std::getline(instream, display_id, ',')) {
//        if (i == 1000)
//            break;

        std::getline(instream, uuid, ',');
        std::getline(instream, document_id, ',');
        std::getline(instream, others);
        int uid = get_uid(uuid);

        display_map.insert({stoi(display_id), std::make_pair(uid, stoi(document_id))});
        //std::cout << display_id << "=> " << uuid << " " << document_id << std::endl;

        if (i % 1000000 == 0)
            std::cout << i/1000000 << "M... " << std::endl;

        ++i;
    }

    file.close();

    std::cout << "\ni = " << i <<"\nTime taken (sec): "
              << std::chrono::duration_cast<std::chrono::seconds>
                      (std::chrono::steady_clock::now() - begin).count()
              << "\n"
              << std::endl;
//    i = 0;
//    for (auto& p: display_map) {
//        if (i == 1000)
//            break;
//
//        std::cout << p.first << " " << p.second.first << " " << p.second.second << std::endl;
//        ++i;
//    }

    return display_map;
}


int calc_user_doc_interaction_topic(
        std::unordered_map<int, std::vector<std::pair<int, float>>> *doc_topic_map,
        std::unordered_map<std::pair<int, int>, float, pairhash> *user_topic_map,
        std::unordered_map<int, std::pair<int, int>> *display_map
)
{
    // read clicks_train
    string filename = "/home/yhk00323/input/clicks_test.csv.gz";
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    std::cout << "Start processing " << filename << std::endl;

    ifstream test_file(filename, ios_base::in | ios_base::binary);
    boost::iostreams::filtering_streambuf<boost::iostreams::input> test_inbuf;
    test_inbuf.push(boost::iostreams::gzip_decompressor());
    test_inbuf.push(test_file);
    std::istream test_instream(&test_inbuf);

    // transform to unordered map
    std::getline(test_instream, line);
    std::cout << "  Headers: " << line << std::endl;

    // write interaction weights
    std::ofstream outfile("clicks_test_doc_topic_weight.csv.gz", std::ios_base::out | std::ios_base::binary);
    boost::iostreams::filtering_streambuf<boost::iostreams::output> out;
    out.push(boost::iostreams::gzip_compressor());
    out.push(outfile);
    std::ostream outstream(&out);

    outstream << "weight\n";

    // for row
    // read clicks_train row
    // save interaction to separate file
    int i = 0;
    while(std::getline(test_instream, display_id, ',')) {
        //if (i == 10000000)
        //    break;
        std::getline(test_instream, others);
        //calculate weight
        float weight = 0.0;
        // if uuid and document id related to the display_id exists
        auto display = (*display_map).find(stoi(display_id));
        if (display != (*display_map).end()) {
            // if topic id related to the document id exists
            auto document = (*doc_topic_map).find(display->second.second);
            for (auto& dt: document->second) {
                // if topic id related to the user id exists
                auto user_topic = (*user_topic_map).find(make_pair(display->second.first, dt.first));
                if (user_topic != (*user_topic_map).end()) {
                    weight += user_topic->second;
                }
            }
        }
        outstream << weight <<"\n";
        ++i;
    }

    test_file.close();

    std::cout << "\ni = " << i <<"\nTime taken (sec): "
              << std::chrono::duration_cast<std::chrono::seconds>
                      (std::chrono::steady_clock::now() - begin).count()
              << "\n"
              << std::endl;

    return 0;
}



int main() {
    // 0. Read file
    std::unordered_map<int, std::vector<std::pair<int, float>>> doc_topic_map = gen_doc_topic_map();

    std::unordered_map<std::pair<int, int>, float, pairhash> user_topic_map = gen_user_topic_map(
            &doc_topic_map);

    std::unordered_map<int, std::pair<int, int>> display_map = gen_display_map();

    // II. calculate user-document interaction
    calc_user_doc_interaction_topic(&doc_topic_map, &user_topic_map, &display_map);



    //

//    // calculate interaction between user and document based on past document topics
//    // I. calculate user-topic interaction based on page_views - a
//    unordered_map<pair<int, int>, float> user_topic_score;
//    // for a page view in page_views:
//    for (auto row = page_view.begin();row != page_view.end();++row) // let's find a way to parallelize
//    {
//        // topic_id = get_topic_id(document_id)
//        int topic_id = doc_topic_list.find(document_id).second;
//        // if score[user_id, topic_id] empty:
//            // score[user_id, topic_id] = topic_confidence_level
//        // else
//            // score[user_id, topic_id] += topic_confidence_level
//            score.find(make_pair(uuid, topic_id)).second += topic_confidence_level;
//    }
//
//
//    // II. calculate user-document interaction based on a
//    // for user in users:
//    for (int i = 0; i <= max(clicks_train);++i)
//    {
//        click row = clicks_train.getrow();
//        // if user's topics are in document's topics:
//        user_topic_list = score.find(make_pair(uuid, topic_id));
//        for (int i = 0; i <= max(user_topic_list); ++i)
//        {
//            user_topic = user_topic_list.getrow();
//
//            doc_topic = doc_topic_list.find(document_id);
//            for (int i = 0; ; ++i)
//            {
//                if (user_topic.topic in doc_topic.topic)
//                // ud_score[user_id, document_id] += score[user_id, topic_id]
//                    ud_score[uuid, document_id] += score[uuid, topic_id];
//            }
//
//        }
//
//    }
//

    // III. save result
    return 0;
}
