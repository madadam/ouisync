#define BOOST_TEST_MODULE utils
#include <boost/test/included/unit_test.hpp>

#include "utils.h"
#include "blob.h"
#include "transaction.h"
#include "user_id.h"
#include "index.h"
#include "block_store.h"

#include <boost/filesystem.hpp>
#include <iostream>

using namespace std;
using namespace ouisync;

Opt<fs::path> g_test_root_dir;

fs::path test_dir(string test_name) {
    namespace fs = ouisync::fs;
    if (!g_test_root_dir) {
        g_test_root_dir = fs::unique_path("/tmp/ouisync/test-%%%%-%%%%");
    }
    return *g_test_root_dir / test_name;
}

BOOST_AUTO_TEST_CASE(blob) {
    {
        Blob blob;

        string test = "hello world";

        blob.write(test.c_str(), test.size(), 0);

        string read(blob.size(), '\0');
        blob.read(&read[0], read.size(), 0);

        BOOST_REQUIRE_EQUAL(test, read);
    }

    {
        Random random;

        Blob blob;

        string test = random.string((1 << 17) + 4329);

        //for (size_t i = 0; i < test.size(); ++i) {
        //    test[i] = 33 + (i % 93);
        //}

        blob.write(test.c_str(), test.size(), 0);

        string read(blob.size(), '\0');
        blob.read(&read[0], read.size(), 0);

        BOOST_REQUIRE_EQUAL(test.size(), read.size());
        BOOST_REQUIRE(test == read);
    }
}

BOOST_AUTO_TEST_CASE(blob_truncate) {
    {
        Random random;

        Blob blob;

        string test = "hello world";

        blob.write(test.c_str(), test.size(), 0);

        auto new_len = strlen("hello");

        blob.truncate(new_len);

        string read(new_len, '\0');
        blob.read(&read[0], read.size(), 0);

        BOOST_REQUIRE_EQUAL(read.size(), new_len);
        BOOST_REQUIRE_EQUAL(read, "hello");
    }

    {
        Random random;

        Blob blob;

        string test = random.string((1 << 17) + 4329);

        blob.write(test.c_str(), test.size(), 0);

        auto new_len = 10;

        blob.truncate(new_len);

        string read(new_len, '\0');
        blob.read(&read[0], read.size(), 0);

        BOOST_REQUIRE_EQUAL(read.size(), new_len);
        BOOST_REQUIRE(read == test.substr(0, new_len));
    }
}

BOOST_AUTO_TEST_CASE(blob_commit) {
    {
        Random random;

        Blob blob;

        string test = "hello world";

        blob.write(test.c_str(), test.size(), 0);

        Transaction tnx;

        blob.commit(tnx);

        BOOST_REQUIRE_EQUAL(tnx.blocks().size(), 1);
        BOOST_REQUIRE_EQUAL(tnx.edges().size(), 0);
    }
}

BOOST_AUTO_TEST_CASE(blob_restore_small) {
    auto uid = UserId::generate_random();

    fs::path dir = test_dir("restore-small-blob");

    Index index(uid, {});
    BlockStore block_store(dir);

    BlockId root;

    string test = "hello world";

    {
        Blob blob;

        blob.write(test.c_str(), test.size(), 0);

        Transaction tnx;

        auto id = blob.commit(tnx);
        tnx.insert_edge(id, id);

        BOOST_REQUIRE_EQUAL(tnx.blocks().size(), 1);
        BOOST_REQUIRE_EQUAL(tnx.edges().size(), 1);

        tnx.commit(uid, block_store, index);

        BOOST_REQUIRE_EQUAL(index.roots().size(), 1);

        root = *index.roots().begin();
    }

    {
        Blob blob = Blob::open(root, block_store);

        BOOST_REQUIRE_EQUAL(blob.size(), test.size());

        string read(blob.size(), 'x');

        blob.read(&read[0], read.size(), 0);

        BOOST_REQUIRE_EQUAL(test, read);
    }
}

BOOST_AUTO_TEST_CASE(blob_restore_big) {
    auto uid = UserId::generate_random();

    fs::path dir = test_dir("restore-big-blob");

    Index index(uid, {});
    BlockStore block_store(dir);

    BlockId root;

    Random random;

    string test = random.string(1 << 17);

    {
        Blob blob;

        blob.write(test.c_str(), test.size(), 0);

        Transaction tnx;

        auto id = blob.commit(tnx);
        tnx.insert_edge(id, id);

        tnx.commit(uid, block_store, index);

        BOOST_REQUIRE_EQUAL(index.roots().size(), 1);

        root = *index.roots().begin();
    }

    {
        Blob blob = Blob::open(root, block_store);

        BOOST_REQUIRE_EQUAL(blob.size(), test.size());

        string read(blob.size(), 'x');

        blob.read(&read[0], read.size(), 0);

        BOOST_REQUIRE_EQUAL(test, read);
    }
}

BOOST_AUTO_TEST_CASE(blob_restore_big_incremental) {
    auto uid = UserId::generate_random();

    fs::path dir = test_dir("restore-big-blob-incrementally");

    Index index(uid, {});
    BlockStore block_store(dir);

    BlockId root;

    Random random;

    string test = random.string(1 << 17);

    {
        Blob blob;

        size_t wrote = 0;
        while (wrote < test.size()) {
            size_t w = std::min<size_t>(test.size() - wrote, 256);
            wrote += blob.write(test.c_str() + wrote, w, wrote);
        }

        Transaction tnx;

        auto id = blob.commit(tnx);
        tnx.insert_edge(id, id);

        tnx.commit(uid, block_store, index);

        BOOST_REQUIRE_EQUAL(index.roots().size(), 1);

        root = *index.roots().begin();
    }

    {
        Blob blob = Blob::open(root, block_store);

        BOOST_REQUIRE_EQUAL(blob.size(), test.size());

        string read(blob.size(), 'x');

        blob.read(&read[0], read.size(), 0);

        BOOST_REQUIRE_EQUAL(test, read);
    }
}