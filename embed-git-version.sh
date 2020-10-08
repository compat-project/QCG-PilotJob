ver=$(git describe --all --long | sed s///\//); sed -i "s/__version__=.*/__version__=\"${ver}\"/" src/qcg/__init__.py
