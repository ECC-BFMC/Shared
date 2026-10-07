Push modifications to submodule

```bash
cd src/data
git status
git add .
git commit -m "Update data module"
git push origin data
cd ../..
git add src/data
git commit -m "Update submodule pointer to latest commit"
git push origin master 
```

Pull modifications from submodule:

```bash
cd src/data
git checkout data
git pull origin data
cd ../..
git add src/data
git commit -m "Update submodule pointer after pulling changes"
git push origin master
```