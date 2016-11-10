const Crawler = require("crawler");
const url = require('url');
const mysql = require('mysql');
const pool = mysql.createPool({
    host: '127.0.0.1',
    user: 'root',
    password: '1',
    database: 'ultrax'
});
var a = 0;
var query=function(sql,callback){
    pool.getConnection(function(err,conn){
        if(err){
            callback(err,null,null);
        }else{
            console.log(sql);
            conn.query(sql,function(qerr,vals,fields){
                //释放连接
                conn.release();
                //事件驱动回调
                callback && callback(qerr,vals,fields);
            });
        }
    });
};
function setDate(date) {
    var d = date ? new Date(date) : new Date();
    return parseInt(d.getTime() / 1000);
}

const homePage = 'http://cd.mamacn.com/forum.php';

const c = new Crawler({
    maxConnections: 10,
    forceUTF8: true,
    headers: {Cookie: "50mT_2132_auth=e249udoWSLX7atKEWUo0fnLkPoA0MHm4CJTM7kNHvC2jOFQhooIOiTehIWJW5wV7tQSSeUMgJxCo4xRnZvwo2gE7MSuILQ"}
});

//首页获取大分类
var homeQueue = function () {
    c.queue({
        uri: homePage,
        callback: function (error, result, $) {
            var data = '';
            $('.fl_tb h2 a').each(function (index, a) {
                var toQueueUrl = $(a).attr('href'),
                        name = $(a).text(),
                        desc = $(a).parent().next().text();
                data = {
                    category: name,
                    desc: desc,
                    uri: toQueueUrl
                };
                setCateQuery(data, function (uri) {
                    categoryQueue(uri);
                })
            });

        }
    });
};

//插入所有分类
function setCateQuery(data, callback) {
    existCate(data.category, function () {
        callback && callback(data.uri);
    })
}

//获取所有分页url
var categoryQueue = function (uri) {
    c.queue({
        uri: uri,
        callback: function (error, result, $) {
            $('#threadlist tbody').length && $('#threadlist tbody').each(function () {
                if ($(this).attr('id') && $(this).attr('id').indexOf('normalthread') != -1) {
                    var href = $(this).find('th a').attr('href'),
                            title = $(this).find('th a').text(),
                            author = $(this).find('.by').eq(0).find('a').text(),
                            cate = $('.xs2>a').text();
                    setThreadQuery(title, author.replace(/\"/g,'\\"'), cate, href,function (user_id, cate_id, sub_id,user,tit,uri) {
                        goToDetail(user_id, cate_id, sub_id, user, tit, uri);
                    });
                }
            })
            if ($('.nxt').length) {
                categoryQueue($('.nxt').attr('href'));
            }
        }
    })
};

//插入帖子
function setThreadQuery(title, author, cate, href,callback) {
    existUser(author, function (user_id) {
        user_id && existCate(cate, function (cate_id) {
            cate_id && existTitle(cate_id, author, user_id, title, function (sub_id) {
                sub_id && callback && callback(user_id, cate_id, sub_id,author,title,href);
            })
        })
    })
}

//详情页获取
var goToDetail = function (user_id, cate_id, sub_id, author,title,href) {
    c.queue({
        uri: href,
        callback: function (error, result, $) {
            var comments = [];
            $('td.t_f').each(function (i) {
                comments.push({
                    content: $(this).text().replace(/\"/g,'\\"'),
                    auther: $('.authi .xw1').eq(i).text().replace(/\"/g,'\\"'),
                    date: $('.authi em').eq(i).text().replace('发表于', '')
                });
            })
            setCommentQuery(author, user_id, cate_id, sub_id, title, comments);
        }
    })
};

//插入评论
function setCommentQuery(author, user_id, cate_id, sub_id, title, comments) {
    comments.length && comments.forEach(function(o,i){
        existComment(author,user_id,cate_id,sub_id,title,o.content,o.date,o.auther,i);
    })
}

//判断用户存在并返回id
function existUser(username, callback) {
    query('select uid from pre_common_member where username = "' + username + '"', function (err, rows) {
        if (!err) {
            if (rows && rows.length) {
                callback && callback(rows[0].uid);
            }
            else {
                query('INSERT INTO pre_common_member (username,password,groupid) VALUES ("' + username + '","123",10);', function (err, rows) {
                    if (!err) {
                        console.log('insert user' + username);
                        var user_id = rows.insertId;
                        query('INSERT INTO pre_common_member_field_forum (uid) VALUES (' + user_id + ')',function(){
                            query('INSERT INTO pre_common_member_field_home (uid) VALUES (' + user_id + ')',function(){
                                query('INSERT INTO pre_common_member_profile (uid) VALUES (' + user_id + ')',function(){
                                    callback && callback(user_id);
                                });
                            });
                        });
                    }
                    else {
                        console.log('NEW USER:' + err);
                    }
                })
            }
        }
        else {
            console.log('SELECT USER:' + err);
        }
    })
}
//判断分类存在并返回id
function existCate(cate, callback) {
    query('select fid from pre_forum_forum where name = "' + cate + '"', function (err, rows) {
        if (!err) {
            if (rows && rows.length) {
                callback && callback(rows[0].fid);
            }
            else {
                query('INSERT INTO pre_forum_forum (fup,type,name,status) VALUES (1,"forum","' + cate + '",1);', function (err, rows) {
                    if (!err) {
                        var cate_id = rows.insertId;
                        callback && callback(cate_id);
                    }
                    else {
                        console.log('NEW CATE:' + err);
                    }
                })
            }
        }
        else {
            console.log('SELECT CATE:' + err);
        }
    })
}
//判断主题存在并返回id
function existTitle(cate_id, username, user_id, title, callback) {
    query('select tid from pre_forum_thread where authorid = ' + user_id + ' and subject = "' + title + '"', function (err, rows) {
        if (!err) {
            if (rows && rows.length) {
                callback && callback(rows[0].tid);
            }
            else {
                query('INSERT INTO pre_forum_thread (fid,author,authorid,subject) VALUES (' + cate_id + ',"' + username + '",' + user_id + ',"' + title + '");', function (err, rows) {
                    if (!err) {
                        var sub_id = rows.insertId;
                        query('update pre_forum_forum set threads = threads+1 where fid=' + cate_id,function(){
                            callback && callback(sub_id);
                        });
                    }
                    else {
                        console.log('NEW THREAD' + err);
                    }
                });
            }
        }
        else {
            console.log('SELECT THREAD:' + err);
        }
    })
}

//判断评论存在并写入
function existComment(author, user_id, cate_id, sub_id, title, content, date, comment_user, i) {
    if (i == 0) {
        query('select pid from pre_forum_post where authorid = ' + user_id + ' and message="' + content + '"', function (err, rows) {
            if (!err) {
                if (rows && !rows.length) {
                    //一楼  楼主 帖子内容
                    query('UPDATE pre_forum_thread set maxposition = maxposition+1,dateline=' + setDate(date) + ',lastposter = "' + author + '" where tid = ' + sub_id, function (err, rows) {
                        if (!err) {
                            console.log('update thread');
                        }
                        else {
                            console.log('UPDATE THREAD:' + err)
                        }
                    });
                    query('INSERT INTO pre_forum_post (fid,tid,author,authorid,subject,message,dateline,position,first,usesig,bbcodeoff,smileyoff) VALUES (' + cate_id + ',' + sub_id + ',"' + author + '",' + user_id + ',"' + title + '","' + content + '",' + setDate(date) + ',' + (i + 1) + ',1,1,-1,-1);', function (err, rows) {
                        if (!err) {
                            console.log('insert post 1' + sub_id);
                            query('update pre_forum_forum set posts = posts+1 where fid=' + cate_id);
                        }
                        else {
                            console.log(err);
                        }
                    });
                }
            }
            else {
                console.log('SELECT post 1 ' + err);
            }
        })
    }
    else {
        existUser(comment_user, function (comment_user_id) {
            comment_user_id && query('select pid from pre_forum_post where authorid = ' + comment_user_id + ' and message="' + content + '"', function (err, rows) {
                if (!err) {
                    if (rows && !rows.length) {
                        query('UPDATE pre_forum_thread set replies = replies+1,maxposition = maxposition+1,dateline=' + setDate(date) + ',lastposter = "' + comment_user + '" where tid = ' + sub_id, function (err, rows) {
                            if (!err) {
                                console.log('update thread');
                            }
                            else {
                                console.log('UPDATE THREAD:' + err)
                            }
                        });
                        query('INSERT INTO pre_forum_post (fid,tid,author,authorid,subject,message,dateline,position,first,usesig,bbcodeoff,smileyoff) VALUES (' + cate_id + ',' + sub_id + ',"' + comment_user + '",' + comment_user_id + ',"","' + content + '",' + setDate(date) + ',' + (i + 1) + ',0,1,-1,-1);', function (err, rows) {
                            if (!err) {
                                console.log('insert post other');
                                query('update pre_forum_forum set posts = posts+1 where fid=' + cate_id);
                            }
                            else {
                                console.log(err);
                            }
                        });
                    }
                }
                else {
                    console.log('SELECT post other ' + err);
                }
            })
        })

    }
}

homeQueue();

