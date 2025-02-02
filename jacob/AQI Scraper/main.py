from utils.args import parse_arguments, ensure_directories_exist
from utils.scraper import get_file_urls


def main():
    args = parse_arguments()

    input_dir = args.input_dir
    output_dir = args.output_dir

    ensure_directories_exist(input_dir, output_dir)

    get_file_urls()


if __name__ == "__main__":
    main()
