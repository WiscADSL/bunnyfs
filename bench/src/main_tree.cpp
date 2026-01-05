#include "fcntl.h"
#include "utils/ufs.h"

void traverse(const std::string& parent_path) {
  auto dir = fs_opendir(parent_path.c_str());
  if (dir == nullptr) throw std::runtime_error("failed to open " + parent_path);

  for (auto de = fs_readdir(dir); de != nullptr; de = fs_readdir(dir)) {
    std::string path = parent_path + de->d_name;

    struct stat st {};
    fs_stat(path.c_str(), &st);
    bool is_dir = S_ISDIR(st.st_mode);

    if (de->d_name[0] == '.') continue;

    if (is_dir) path += "/";
    fmt::print("{:28}\t {:10L} B\t (inode={:4}, mode={:6o})\n", path,
               st.st_size, de->d_ino, st.st_mode);
    if (is_dir) traverse(path);
  }

  fs_closedir(dir);
}

int main() {
  UFSContext ctx;

  // Use the default locale to print file sizes with separators.
  std::locale::global(std::locale(""));

  fmt::print("{:=^80}\n", "Start Tree");
  traverse("/");
  fmt::print("{:=^80}\n", "End Tree");
}
