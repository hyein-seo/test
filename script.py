

if __name__ == '__main__':
    import argparse

    from dbr_test import main

    parser = argparse.ArgumentParser()

    parser.add_argument('--game_code', type=str)
    parser.add_argument('--env_code', type=str)
    parser.add_argument('--log_type', type=str)
    parser.add_argument('--target_date', type=str)
    parser.add_argument('--config', type=str)
    parser.add_argument('--config_test', type=str)
    parser.add_argument('--test', action='store_true', default=False)
    parsed_args = parser.parse_args()

    print(parsed_args)
    main.run(parsed_args)
