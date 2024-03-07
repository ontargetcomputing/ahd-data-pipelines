from ahd_data_pipelines.uppercase import uppercase

# I am doing this with this method

def uppercasethename(name: str):
    my_uppercase_name = uppercase(name)
    return my_uppercase_name


def main():
  name = uppercasethename("rich")

  print(name)

if __name__ == '__main__':
  main()