import argparse
from src.pipelines import process_bancos, process_empregados, process_reclamacoes, join_and_load

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", required=True, choices=["bancos", "empregados", "reclamacoes", "join_and_load"])
    args = parser.parse_args()

    if args.step == "bancos":
        process_bancos.run()
    elif args.step == "empregados":
        process_empregados.run()
    elif args.step == "reclamacoes":
        process_reclamacoes.run()
    elif args.step == "join_and_load":
        join_and_load.run()

if __name__ == "__main__":
    main()