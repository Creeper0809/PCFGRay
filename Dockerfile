FROM rayproject/ray:2.47.0-py3.11

# 필요 패키지 설치
RUN pip install \
    numpy \
    git+https://github.com/creeper0809/PCFGCracking.git@master#egg=pcfg-lib