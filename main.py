import multiprocessing

from sqlalchemy import text

from db import get_db


def _get_recipes_for_fact(_db):
    """


    :param _db: SQLAlchemy database session
    :return: list[dict(int, datetime, int)]

    gets every recipe from recipe table for recipe fact

    """
    rows = _db.execute(text("SELECT id, created_at, user_id FROM recipe"))
    return rows.mappings().all()


def _get_recipe_dims(_dw):

    """


    :param _dw: SQLAlchemy database session
    :return: list[recipe]

    needed to find the right recipe_key value for recipe_fact

    """

    rows = _dw.execute(text("SELECT * FROM recipe_dim"))
    return rows.mappings().all()


def _get_dates(_dw):
    """


        :param _dw: SQLAlchemy database session
        :return: list[recipe]

        needed to find the right recipe_key value for fact tables

        """

    rows = _dw.execute(text("SELECT * FROM date_dim"))
    return rows.mappings().all()


def _get_date_key(item, dates, key='created_at'):

    """


    :param item: either recipe or cooking table row
    :param dates: rows of date_dim talbe
    :param key: name of the datetime column
    :return: Optional[date_key: int]

    finds the matching date_key value out of all date_dim rows

    """

    date = item[key]

    for d in dates:

        if date.year == d['year'] and date.month == d['month'] and date.isocalendar().week == d['week'] and date.hour == \
                d['hour'] and date.minute == d['minute'] and date.second == d['second']:
            return d['date_key']

    return None


def _get_users(_db):

    """

    :param _db: SqlaAlchemy database session
    :return: list[reseptit.user]

    extracts users from reseptit database

    """

    rows = _db.execute(text("""SELECT users.id AS user_id, username, auth_roles.id AS role_id, role 
        FROM users INNER JOIN auth_roles ON auth_roles.id = users.auth_role_id
    """))

    users = rows.mappings().all()
    return users


def _get_user_dims(_dw):

    """

    :param _dw: SQLAlchemy database session
    :return: list[resepti_olap.user]
    fetch data from user_dim table to find the right user_dim.user_key in fact tables

    """

    rows = _dw.execute(text("""SELECT user_key, user_id FROM user_dim"""))
    return rows.mappings().all()


def _get_recipes(_db):

    """

    :param _db: SQLAlchemy database session
    :return: list of [reseptit.recipe]
    """

    rows = _db.execute(text("""SELECT recipe.id AS recipe_id, recipe.name, users.username AS user, users.id AS user_id,categories.id AS category_id, categories.name AS category 
        FROM recipe INNER JOIN users ON recipe.user_id = users.id
        INNER JOIN categories ON recipe.category_id = categories.id"""))

    return rows.mappings().all()


def recipes_etl(child):

    """

    :param child: child connection of multpiprocessing.Pipe() to signal main process if everything's ok
    :return: None

    this is ran in a child process inserts every row in oltp reseptit.recipe-table into recipe_dim talble

    """

    with get_db() as _db:
        recipes = _get_recipes(_db)

    with get_db(_type='dw') as _dw:
        for recipe in recipes:
            _dw.execute(text("""
            INSERT INTO recipe_dim(recipe_id, name, user, user_id, category_id, category, current) VALUES(:recipe_id, :name, :user, :user_id, :category_id, :category, :current)
            """), {'recipe_id': recipe['recipe_id'], 'name': recipe['name'], 'user': recipe['user'],
                   'user_id': recipe['user_id'], 'category_id': recipe['category_id'], 'category': recipe['category'],
                   'current': 1})

        try:
            _dw.commit()
            child.send('recipes done')
        except Exception as e:
            _dw.rollback()
            child.send(f'error with recipes {str(e)}')
        finally:
            child.close()


def users_etl(child):
    """

        :param child: child connection of multpiprocessing.Pipe() to signal main process if everything's ok
        :return: None

        this is ran in a child process inserts every row in oltp reseptit.users-table into user_dim table

        """

    with get_db() as _db:
        users = _get_users(_db)

    with get_db(_type='dw') as _dw:
        for user in users:
            _dw.execute(text(
                """INSERT INTO user_dim (user_id, username, role_id, role, current) VALUES (:user_id, :username, :role_id, :role, :current)"""),
                {
                    'user_id': user['user_id'],
                    'username': user['username'],
                    'role_id': user['role_id'],
                    'role': user['role'],
                    'current': 1})

        try:
            _dw.commit()
            child.send('users done')
        except Exception as e:
            _dw.rollback()
            child.send(f'users error {e}')
        finally:
            child.close()


def _get_recipe_dates(_db):

    """

    :param _db: SQLAlchemy database
    :return: list of created_at datetimes in reseptit.recipe table
    """

    rows = _db.execute(text("SELECT DISTINCT created_at AS dt FROM recipe"))
    date_rows = rows.mappings().all()
    dates = []
    for row in date_rows:
        dates.append(row['dt'])
    return dates


def _get_cooking_dates(_db):
    """

        :param _db: SQLAlchemy database
        :return: list of cooked_date datetimes in reseptit.cooking table
        """

    rows = _db.execute(text("SELECT DISTINCT cooked_date AS dt FROM cooking"))
    date_rows = rows.mappings().all()
    dates = []
    for row in date_rows:
        dates.append(row['dt'])
    return dates


def dates_etl(child):
    """

            :param child: child connection of multpiprocessing.Pipe() to signal main process if everything's ok
            :return: None

            this is ran in a child process inserts dates in oltp reseptit.recipe-table and cooking-table into to date_dim-table

            """


    with get_db() as _db:
        # get's dates from oltp databse tables
        recipe_dates = _get_recipe_dates(_db)
        cooking_dates = _get_cooking_dates(_db)


        # merges two lists into one
        all_dates = recipe_dates + cooking_dates
        # creates a set out of the list to get rid of duplicate dates
        # and creates a new list of unique dates
        unique_dates_set = set(all_dates)
        unique_dates = list(unique_dates_set)

    # insert every datetime into date_dim
    with get_db(_type='dw') as _dw:
        for date in unique_dates:
            _dw.execute(text("""
            INSERT INTO date_dim(year,month, week, day, hour, minute, second)
            VALUES(:year, :month, :week, :day, :hour, :minute, :second)
            """), {
                'year': date.year,
                'month': date.month,
                'week': date.isocalendar().week,
                'day': date.day,
                'hour': date.hour,
                'minute': date.minute,
                'second': date.second
            })
        try:
            _dw.commit()
            child.send('dates done')
        except Exception as e:
            _dw.rollback()
            child.send(f'error insreting dates {str(e)}')
        finally:
            child.close()


def _get_recipe_key(oltp_item, recipe_dims, key='id'):

    """

    :param oltp_item: either cooking or recipe from oltp-database
    :param recipe_dims: recipes from recipe_dim table
    :param key: key name id or recipe_id
    :return: Optional[recipe_key:int]

    finds right recipe_key to be used in cooking_fact and recipe_fact tables as a foreign key value

    """

    for dim in recipe_dims:
        if oltp_item[key] == dim['recipe_id']:
            return dim['recipe_key']

    return None


def _get_user_key(oltp_item, user_dims):

    """

    :param oltp_item: user from oltp-database
    :param user_dims: rows in user_dim table
    :return: Optional[user_key:int]

    finds the right user_key from user_dim to be used in fact tables as a foreign key

    """

    for dim in user_dims:
        if oltp_item['user_id'] == dim['user_id']:
            return dim['user_key']
    return None


def _get_cooking_for_fact(_db):
    rows = _db.execute(text('SELECT * FROM cooking'))
    return rows.mappings().all()


def cooking_fact_etl(child):
    """

            :param child: child connection of multpiprocessing.Pipe() to signal main process if everything's ok
            :return: None

            this is ran in a child process inserts every row in oltp reseptit.users-cooking into cooking_fact table

            """

    with get_db() as _db:
        cookings = _get_cooking_for_fact(_db)

    with get_db(_type='dw') as _dw:
        try:
            date_dims = _get_dates(_dw)
            user_dims = _get_user_dims(_dw)
            recipe_dims = _get_recipe_dims(_dw)
            for cooking in cookings:
                _date_key = _get_date_key(cooking, date_dims, key='cooked_date')
                _user_key = _get_user_key(cooking, user_dims)
                _recipe_key = _get_recipe_key(cooking, recipe_dims, key='recipe_id')
                if _date_key is None or _recipe_key is None or _recipe_key is None:
                    continue

                _dw.execute(text(
                    'INSERT INTO cooking_fact(date_cooked, user, recipe, rating) VALUES (:date_cooked, :user, :recipe, :rating)'),
                    {'date_cooked': _date_key, 'user': _user_key, 'recipe': _recipe_key,
                     'rating': cooking['rating']})
            _dw.commit()
            child.send('inserting cooking facts ok')
        except Exception as e:

            _dw.rollback()
            child.send(f'error inserting cooking facts {str(e)}')
        finally:
            child.close()


def recipe_fact_etl(child):
    """

            :param child: child connection of multpiprocessing.Pipe() to signal main process if everything's ok
            :return: None

            this is ran in a child process inserts every row in oltp reseptit.recipe-table into recipe_dim table

            """

    with get_db() as _db:
        recipes = _get_recipes_for_fact(_db)

    with get_db(_type='dw') as _dw:
        try:
            recipe_dims = _get_recipe_dims(_dw)
            user_dims = _get_user_dims(_dw)
            dates = _get_dates(_dw)
            for recipe in recipes:
                _date_key = _get_date_key(recipe, dates)
                _recipe_key = _get_recipe_key(recipe, recipe_dims)
                _user_key = _get_user_key(recipe, user_dims)

                if _date_key is None or _recipe_key is None or _user_key is None:
                    continue

                _dw.execute(text(
                    "INSERT INTO recipe_fact(created_at, fact_column, recipe, user) VALUES(:creaetd_at, :fact_column, :recipe, :user)"),
                    {
                        'creaetd_at': _date_key,
                        'fact_column': 1,
                        'recipe': _recipe_key,
                        'user': _user_key
                    })
            _dw.commit()
            child.send('recipe fact ok')
        except Exception as e:
            child.send(f'error insertnig recipe fact {str(e)}')
            _dw.rollback()
        finally:
            child.close()


def main():
    with get_db(_type='dw') as _dw:
        try:
            # this example is eimplified so at the beginning of every etl run, remove data from dw
            _dw.execute(text('DELETE FROM cooking_fact'))
            _dw.execute(text('DELETE FROM recipe_fact'))
            _dw.execute(text('DELETE FROM date_dim'))
            _dw.execute(text('DELETE FROM recipe_dim'))

            _dw.execute(text('DELETE FROM user_dim'))
            _dw.commit()
        except Exception as e:
            print(e)
            _dw.rollback()

    # create pipes to enable communication between main and child processes
    users_prnt, users_child = multiprocessing.Pipe()
    recipes_prnt, recipes_child = multiprocessing.Pipe()
    dates_prnt, dates_child = multiprocessing.Pipe()

    # make every etl run a sub process
    processes = [


        {'p': multiprocessing.Process(target=recipes_etl, args=(recipes_child,)), 'parent': recipes_prnt},
        {'p': multiprocessing.Process(target=dates_etl, args=(dates_child,)), 'parent': dates_prnt},
        {'p': multiprocessing.Process(target=users_etl, args=(users_child,)), 'parent': users_prnt},

    ]
    # start processes
    for process in processes:
        process['p'].start()
        print("############### started")
        # print(process['parent'].recv())

    # receive messages from child processes
    for process in processes:
        print(process['parent'].recv())

    # after having finsihed, join all the child processes into main process
    # ie. wait for all the child processes (etl runs) to finish
    for process in processes:

        process['p'].join()

    # after having inserted all the dimensional data
    # create pipes for fact etl processes
    recipes_prnt, recipes_child = multiprocessing.Pipe()
    cooking_prnt, cooking_child = multiprocessing.Pipe()

    # make every fact etl run a sub process
    processes = [

        {'p': multiprocessing.Process(target=recipe_fact_etl, args=(recipes_child,)), 'parent': recipes_prnt},
        {'p': multiprocessing.Process(target=cooking_fact_etl, args=(cooking_child,)), 'parent': cooking_prnt},

    ]

    # start them
    for process in processes:
        process['p'].start()

    # wait for fact etl processes to finish
    for process in processes:
        process['p'].join()

    print("############### all done")


if __name__ == '__main__':
    main()
